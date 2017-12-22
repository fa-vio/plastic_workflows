import datetime
import logging

# cluster ssh and rsync commands need thread-safe subprocess
import subprocess32
import threading
import time
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from functools import partial, wraps
from itertools import count
from operator import attrgetter, itemgetter
from copy import copy

import paramiko
from paramiko import SSHClient

# import boto3 headers here
from botocore.exceptions import ClientError

# services contains creation methods for AWS services used here
from provisioning.aws.services import Service
from provisioning.aws.ec2_instance import create_ec2_spot_instances, create_ec2_instances,\
    wait_spot_requests_fullfilled

from utils import UserError, std_dev, mean, camel_to_snake, randomizeID, Map,\
    to_aws_name, waitForOpenPort, privateToPublicKey
from provisioning.aws import defaultType, BestAvZone


log = logging.getLogger( "cloud_provision" )


class fabric_task( object ):
    '''
    Create a factory object (aka a Singleton) that executes commands on remote
    instances. This class can be used as a decorator for other functions (e.g.,
    '@fabric_task'), and the '__call__' body gets executed with the arguments
    of the wrapped function.

    Every operation to be performed on a remote instance passes through
    this object, uses a reentrant lock for thread-safety and keeps track of
    the users performing operations.
    Functions through this class can be called only from subclasses of
    'Engine'
    '''

    # A stack to stash the current fabric user before a new one is set via this decorator
    user_stack = [ ]
    # A reentrant lock to prevent multiple concurrent uses of fabric, which is not thread-safe
    lock = threading.RLock( )

    def __new__( cls, user=None ):
        if callable( user ):
            return cls( )( user )
        else:
            return super( fabric_task, cls ).__new__( cls )

    def __init__( self, user=None ):
        self.user = user

    def __call__( self, function ):
        @wraps( function )
        def wrapper( engine, *args, **kwargs ):
            with self.lock:
                user = engine.admin_account( ) if self.user is None else self.user
                user_stack = self.user_stack
                if user_stack and user_stack[ -1 ] == user:
                    return function( engine, *args, **kwargs )
                else:
                    user_stack.append( user )
                    try:
                        task = partial( function, engine, *args, **kwargs )
                        task.name = function.__name__
                        # noinspection PyProtectedMember
                        return engine._execute_task( task, user )
                    finally:
                        assert user_stack.pop( ) == user

        return wrapper


class Engine( object ):
    """
    Manage EC2 instances. Each instance of this class represents a single virtual machine (aka
    instance) in EC2.

    Note that this is an abstract class in python: it must be subclassed
    """

    __metaclass__ = ABCMeta

    Release = namedtuple( 'Release', ('codename', 'version', 'ami') )

    @classmethod
    def role( cls ):
        """
        The name of the role performed by instances of this class, or rather by the EC2 instances
        they represent.
        """
        return camel_to_snake( cls.__name__, '-' )

    @abstractmethod
    def this_release( self ):
        """
        Returns the Release information of the current engine
        A release is characterised by three params:
            codename; version; ami_id
        """
        raise NotImplementedError( )

    @abstractmethod
    def admin_account( self ):
        """
        Returns the name of a user that has sudo privileges. All administrative commands on the
        engine are invoked via SSH as this user.
        """
        raise NotImplementedError( )

    def default_account( self ):
        """
        Returns the name of the user with which interactive SSH session are started on the engine.
        The default implementation forwards to self.admin_account().
        """
        return self.admin_account( )

    def _image_name_prefix( self ):
        """
        Returns the prefix to be used for naming images created from this engine
        """
        return self.role( )

    class NoSuchImageException( RuntimeError ):
        pass

    @abstractmethod
    def base_image( self ):
        """
        Returns the default base image that boxes performing this role should be booted from
        before they are being setup

        :rtype: boto3.ec2.Image
        """
        raise NotImplementedError( )

    @abstractmethod
    def setup( self, **kwargs ):
        """
        Create the EC2 instance represented by this engine, install OS and additional packages on,
        optionally create an AMI image of it, and/or terminate it.
        """
        raise NotImplementedError( )


    def __init__( self, env=None ):
        """
        :param env: a Service object, needed to access all EC2 resources
                available to the current user.
        """

        self.env = env
        # The actual session environment. This is the object that encapsulates
        # all the settings used by AWS instances. Further, it permits to manage
        # those resources.

        self.release_info = None
        # Linux distro's release to use. This affects the image_id to be used

        self.image_id = None
        # The image the instance was or will be booted from

        self.__instance = None
        # The instance represented by this engine

        self.generation = None
        # The number of previous generations of this engine. When an instance
        # is booted from a stock AMI, generation is 0. After that instance is
        # set up and imaged and another instance is booted from the resulting
        # AMI, generation will be 1.

        self.cluster_ordinal = None
        # The ordinal of this engine within a cluster of boxes. For boxes that
        # don't join a cluster, this will be 0

        self.cluster_name = None
        # The name of the cluster this engine is a node of, or None if this
        # engine is not in a cluster.

        self.placement_group = None
        # The placement group where this engine is placed, if in a cluster

        self.role_options = { }
        # Role-specifc options for this engine

        self.key_in_use = None
        # path to the SSH used to create and to SSH to the instance

        if self.env is None:
            raise UserError( "A Service is required before creating any engine instance. "
                             "In order to create the Service object, be sure to put valid AWS "
                             "credentials in environment variables or in ~/.aws/credentials. "
                             "For details, refer to %s."
                             % 'http://boto3.readthedocs.io/en/latest/guide/configuration.html' )
        else:
            assert isinstance(self.env, Service)

    @property
    def instance( self ):
        """
        :rtype: EC2.Instance
        """
        if not self.__instance:
            return None
        return self.__instance


    @property
    def instance_id( self ):
        if self.__instance:
            return self.instance.instance_id
        else:
            return None

    @property
    def ip_address( self ):
        return self.instance.public_ip_address

    @property
    def private_ip_address( self ):
        """
        Set by bind() and create(), the private IP address of this instance
        """
        return self.instance.private_ip_address

    @property
    def host_name( self ):
        return self.instance.public_dns_name

    @property
    def launch_time( self ):
        return self.instance.launch_time

    @property
    def state( self ):
        # could be one among:
        # 0 : pending
        # 16 : running
        # 32 : shutting-down
        # 48 : terminated
        # 64 : stopping
        # 80 : stopped
        return self.instance.state

    @property
    def zone( self ):
        '''
        The location where the instance launched, if applicable.
        returns a dict containing:
            {AvailabilityZone; Affinity; GroupName; HostId; Tenancy; SpreadDomain}
        '''
        return self.instance.placement

    @property
    def role_name( self ):
        return self.role( )

    @property
    def instance_type( self ):
        return self.instance.instance_type

    def prepare(self, keyName=None, instance_type=None, image_id=None, num_instances=1,
                spot_bid=None, spot_auto_tune=False, **options):
        """
        Prepare the environment to create EC2 instance(s) out of this engine.
        Based on the given parameters, it elaborates all required features
        for the new instance according to the resources available to the
        current AWS user.

        :param keyName: the AWS key pair to use, in order to create the
                instance and access it via SSH.
        :param instance_type: the type of instance to create (default:
                m3.medium).
        :param image_id: AMI ID of the image to boot from. If None, the
                return value of
                self._base_image() will be used.
        :param num_instances: the number of instances to prepare (default: 1)
        :param spot_bid: bid for spot instances in dollars. If None, on-demand
                instance(s) will be created. If None and spot_auto_tune is True,
                spot price will be chosen automatically.
        :param spot_auto_tune: boolean to tell whether to choose the "best"
                price and the best availability zone to launch spot instances in.
                Overrides the availability zone in the context.
        :param dict options: Additional, role-specific options can be specified.


        :return: Map: a dictionary with keyword arguments that can be used to
                    create the instance.
        """

        if self.instance_id is not None:
            raise AssertionError( 'Instance already bound or created' )

        if instance_type is None:
            instance_type = defaultType

        if not keyName:
            keyName, keyPath = self.env.get_key_pair()
        else:
            keyName, keyPath = self.env.get_key_pair(keyName)

        self.key_in_use = keyPath

        if image_id is None:
            self.image_id = self.__get_image( )
        else:
            self.image_id = image_id

        zone = self.env.availability_zone
        stamp = str(datetime.datetime.now())
        pl_group_name = 'plgroup_' + zone + '_' + randomizeID(stamp)
        pl_group = self.env.ec2.create_placement_group(
                    GroupName=pl_group_name,
                    Strategy='cluster'
                )

        placement = Map(AvailabilityZone=zone,GroupName=pl_group_name)
        sec_groups_ids = self.__setup_security_groups()

        subnets = self.env.ec2.subnets.filter(
                        Filters=[{'Name' : 'availability-zone', 'Values' : [zone]}]
                    )


        subnet_id = [s.id for s in subnets]

        if spot_auto_tune:
            spot_details = self.__fix_spot(instance_type=instance_type,
                        bid=spot_bid)
            placement.AvailabilityZone=spot_details.name
            spot_bid = spot_details.price_deviation

        arguments = Map(
            ImageId=self.image_id,
            MinCount=1,
            MaxCount=num_instances,
            InstanceType=instance_type,
            KeyName=keyName,
            SecurityGroupIds=sec_groups_ids,
            SubnetId=subnet_id[0],
            Placement=placement,
            BidPrice=spot_bid
        )

        return arguments

    def create(self, arguments,
               terminate_on_error=True,
               cluster_ordinal=0,
               user_data=None,
               executor=None ):
        """
        Create the EC2 instance(s) represented by this engine. Optionally
        wait for the instance(s) to be ready.

        If multiple instances were required from this engine, one clone
        for each additional instance will be created. This engine instance
        will represent the first EC2 instance, while the __clone_instance will represent
        the additional EC2 instances.
        The given executor will be used to handle post-creation activity on
        each instance.

        :param arguments: a Map dictionary with keyword arguments to pass to
                EC2 in order to create the instances

        :param terminate_on_error: If True, terminate instance on errors. If
                False, never terminate any instances. Unfulfilled spot
                requests will always be cancelled.

        :param cluster_ordinal: the cluster ordinal to be assigned to the
                first instance

        :param user_data: path to a user data script (bash script) that can be
                used to configure the instance at launch time.

        :param executor: a callable that accepts two arguments: a task
                function and a sequence of task arguments. The executor
                applies the task function to the given sequence of arguments.
                It may choose to do so synchronously or asynchronously. If
                None, a synchronous executor will be used by default.
        """

        if isinstance( cluster_ordinal, int ):
            cluster_ordinal = count( start=cluster_ordinal )

        if executor is None:
            def executor( f, args ):
                f( *args )

        engines = [ ]
        pending_ids = set( )
        pending_ids_lock = threading.RLock( )

        def store_instance( instance ):
            pending_ids.add( instance.id )
            self.embed( instance, next( cluster_ordinal ) )
            engines.append( instance )

        if user_data:
            import base64
            user_text = base64.b64encode(
                bytes(open(user_data,'r').read()
                ))#.decode('ascii')

        try:
            if arguments.BidPrice:
                price = arguments.BidPrice
                del arguments.BidPrice

                instances = create_ec2_spot_instances( spot_price=price,
                                                       env=self.env,
                                                       imageId=self.image_id,
                                                       count=arguments.MaxCount,
                                                       secGroup=arguments.SecurityGroupIds,
                                                       instType=arguments.InstanceType,
                                                       keyName=arguments.KeyName,
                                                       Placement=arguments.Placement,
                                                       subnet=arguments.SubnetId,
                                                       usr_data=user_text
                                                    )
                for spot in instances['SpotInstanceRequests']:
                    inst_id = wait_spot_requests_fullfilled(self.env, spot['SpotInstanceRequestId'])
                    inst = self.env.ec2.Instance(inst_id)
                    store_instance(inst)
            else:
                instances = create_ec2_instances( env=self.env,
                                                  imageId=self.image_id,
                                                  count=arguments.MaxCount,
                                                  instType=arguments.InstanceType,
                                                  secGroup=arguments.SecurityGroupIds,
                                                  keyName=arguments.KeyName,
                                                  Placement=arguments.Placement,
                                                  subnet=arguments.SubnetId,
                                                  usr_data=user_text
                                                )
                for inst in instances:
                    store_instance( inst )
        except ClientError as e:
            log.error("Received an error creating instances: %s", e, exc_info=True )
            if terminate_on_error:
                with pending_ids_lock:
                    if pending_ids:
                        log.warn( 'Terminating instances ...' )
                        for p_id in pending_ids:
                            self.env.ec2.Instance(p_id).terminate()
                raise
            else:
                with pending_ids_lock:
                    pending_ids.remove( self.instance_id )
                raise

        for inst in engines:
            inst.load()
            log.info("Waiting for instance %s to be running..." , inst.id)
            inst.wait_until_running()
            waitForOpenPort(inst.public_ip_address)
            time.sleep(2)

        return engines


    def embed( self, instance, cluster_ordinal ):
        """
        Link the given EC2 instance with this engine.
        Tag the instance with additional information
        """
        self.__instance = instance
        self.cluster_ordinal = cluster_ordinal
        if self.cluster_name is None:
            self.cluster_name = 'ClusterXX-' + randomizeID(a_str=None, num_digits=4)
        self.__tag_created_instance()


    def remaining_billing_interval(self):
        """
        Amazon uses hourly billing cycles. This function checks how much time
        we have left, before we are charged again for the instance.

        :param instance: boto3.EC2.Instance
        """

        self.instance.load()
        launch_time = self.instance.launch_time
        launch_time = launch_time.replace(tzinfo=None)
        now = datetime.datetime.utcnow()
        delta = now - launch_time

        return 1.0 - ((delta.total_seconds() / 3600.0) % 1.0)


    def __clone_instance( self ):
        """
        Generates infinite numbers of __clone_instance of this box.

        :rtype: Iterator[Box]
        """
        while True:
            clone = copy( self )
            clone.unbind( )
            yield clone


    def __assert_state( self, expected_state ):
        """
        Raises a UserError if the instance is not in the expected state.

        :param expected_state: the expected state
        :return: the instance
        """
        actual_state = self.instance.state
        if actual_state != expected_state:
            raise UserError( "Expected instance state '%s' but got '%s'"
                             % (expected_state, actual_state) )


    def __tag_created_instance( self, inst_id=None ):
        """
        Tags created instances with additional information regarding their
        role and function
        """
        if not inst_id:
            inst_id = self.instance_id

        name = self.role() + '-' + randomizeID(a_str=None, num_digits=4)
        self.env.ec2client.create_tags(
                    Resources=[inst_id],
                    Tags=[{'Key':'Name','Value':to_aws_name(name)},
                          {'Key':'Generation','Value':str(self.generation)},
                          {'Key':'Cl_Ordinal','Value':str(self.cluster_ordinal)},
                          {'Key':'Cl_Name','Value':self.cluster_name}
                    ]
        )


    def __security_group_name( self ):
        """
        Override the security group name to be used for this engine
        """
        return self.role( )


    def __setup_security_groups( self, vpc_id=None, **rules ):
        '''
        Creates security groups (inbound and outbound), the boto3 way.
        Briefly, everything can exit towards everywhere; Mesos and
        Zookeeper ports are open; http port is open; ssh port is open

        :return: list with Id of security groups created
        '''
        log.info( 'Setting up security group...' )
        #sg = self.env.ec2.SecurityGroup('sg-12345678')
        #try:
        sg = self.env.ec2.create_security_group(
                Description="Security group for engine of role %s" % self.role(),
                GroupName= self.role() + '-' + randomizeID(str(time.time())),
                VpcId= vpc_id if vpc_id is not None else self.env.vpc.id,
                DryRun = False )

        #except ClientError as e:
        #    log.error("Received an error creating security group: %s", e, exc_info=True )

        # It's OK to have two security groups of the same name as long as their
        # VPC is distinct. Assert vpc_id is None or sg.vpc_id == vpc_id
        #erules = self.__populate_egress_rules( )
        inrules = self.__populate_ingress_rules( )

        if sg:
            #sg.authorize_egress( IpPermissions=erules, DryRun=False)
            sg.authorize_ingress( IpPermissions=inrules, DryRun=False )
            return [sg.id]
        else:
            log.error( "Problems creating the security group %s", self.role() )
            return None


    def __populate_egress_rules( self, **rules ):
        """
        :return: A list of rules, each rule is a dict with keyword arguments to
        boto3.ec2.SecurityGroup, namely

        FromPort
        GroupName
        ToPort
        CidrIp
        SourceSecurityGroupName
        SourceSecurityGroupOwnerId
        IpPermissions (list)
        IpProtocol
        """
        ipRange = dict( CidrIp='0.0.0.0/0', Description='To everywhere' )
        ipPerms = [ dict( IpProtocol='-1', FromPort=-1, ToPort=-1, IpRanges=[ipRange] ), rules ]

        return ipPerms


    def __populate_ingress_rules( self, **rules ):
        """
        :return: A list of ingress rules, each rule is a dict with keyword arguments to
        boto3.ec2.SecurityGroup, namely

        IpProtocol
        FromPort
        ToPort
        CidrIp
        SourceSecurityGroupName
        SourceSecurityGroupOwnerId
        IpPermissions (list)
        """
        ipRange = dict( CidrIp='0.0.0.0/0', Description='From everywhere' )
        ipPerms = [ dict( IpProtocol='tcp', FromPort=80, ToPort=80, IpRanges=[ipRange] ),
                   dict( IpProtocol='tcp', FromPort=443, ToPort=443, IpRanges=[ipRange] ),
                   dict( IpProtocol='tcp', FromPort=5051, ToPort=5051, IpRanges=[ipRange] ),
                   dict( IpProtocol='tcp', FromPort=5050, ToPort=5050, IpRanges=[ipRange] ),
                   dict( IpProtocol='tcp', FromPort=22, ToPort=22, IpRanges=[ipRange] ),
                   rules ]

        return ipPerms

    def make_image( self ):
        """
        Create an AMI image of the EC2 instance attached to this engine and
        return its ID.
        This instance uses an EBS-backed root volume.

        Instance must be stopped before creating the image.
        """
        # We've observed instance state to flap from stopped back to stoppping. As a best effort
        # we wait for it to flap back to stopped.
        self.instance.wait_until_stopped()

        log.info( "Creating image ..." )
        timestamp = str(datetime.datetime.now())
        timestamp = timestamp.split('.')[0].replace('-', '').replace(':', '').replace(' ', '-')

        image_name = to_aws_name( self._image_name_prefix( ) + "_" + timestamp )

        image_id = self.env.ec2client.create_image(
            BlockDeviceMappings=[],
            Description="Custom AMI for cloud provision",
            InstanceId=self.instance_id,
            Name=image_name
        )

        while True:
            try:
                image = self.env.ec2.images.filter(ImageIds=[image_id] )
                self.__tag_created_instance( image_id )
                image[0].wait_until_exists()
                log.info( "... created %s (%s).", image[0].id, image[0].name )
                break
            except ClientError as e:
                log.error("Received an error creating the image: %s", e, exc_info=True )
                raise

        return image_id


    def stop( self ):
        """
        Stop the EC2 instance. Stopped instances can be started later using
        :py:func:`Box.start`.
        """
        self.__assert_state( 'running' )
        log.info( 'Stopping instance ...' )
        self.instance.stop( DryRun = False )
        self.instance.wait_until_stopped()
        log.info( '...instance stopped.' )


    def start( self ):
        """
        Start the EC2 instance represented by this engine
        """
        self.__assert_state( 'stopped' )
        log.info( 'Starting instance ... ' )
        self.instance.start( DryRun = False )

        self.instance.wait_until_running()
        log.info( '... instance is running.' )

    def reboot( self ):
        """
        Reboot the EC2 instance represented by this engine. When this method returns,
        the EC2 instance represented by this object will likely have different public IP and
        hostname.
        """
        self.instance.reboot()

    def terminate( self, wait=True ):
        """
        Terminate the EC2 instance represented by this engine.
        """
        log.info( 'Terminating instance ...' )
        self.instance.terminate( )
        self.instance.wait_until_terminated()
        log.info( '... instance terminated.' )

    def run_command_on_instance(self, client, command):
        '''
        Runs commands on the remote linux instance

        :param client: a paramiko SSH client
        :param command: the command to be executed
        '''
        try:
            stdin, stdout, stderr = client.exec_command( command )

            if stderr:
                log.error("[STDERR] %s", stderr.read())

            if stdout:
                log.info("[STDOUT] %s", stdout.read())

        except paramiko.SSHException as e:
            log.error("Error while running command on %s:\n %s", self.instance_id, e)


    def get_via_ssh(self, remotepath, localpath, key_file):
        '''
        Copy a remote file (remotepath) the local host (localpath).
        Note that paths must be absolute paths to files
        '''
        client = self.__ssh_client(key_file)
        try:
            sftp = client.open_sftp()
            sftp.get(remotepath,localpath)
            sftp.close()
        except Exception as e:
            log.error("Error getting from remote host: %s", e)

        if client:
            client.close()


    def put_via_ssh(self, remotepath, localpath, key_file):
        '''
        Copy a local file (localpath) the remote host (remotepath).
        Note that paths must be absolute paths to files
        '''
        client = self.__ssh_client(key_file)
        try:
            sftp = client.open_sftp()
            sftp.put(localpath, remotepath)
            sftp.close()
        except Exception as e:
            log.error("Error getting from remote host: %s", e)

        if client:
            client.close()


    def __write_authorized_keys(self, key_file):
        '''
        get a paramiko SSH client - i.e. a session with a SSH server
        if a path to SSH key is specified, the client will connect
        using that key

        :param key_file: path to the SSH key file needed to connect
        '''

        key_pub = privateToPublicKey(key_file)

        client = self.__ssh_client(key_file)

        self.run_command_on_instance(client,'mkdir -p ~/.ssh/')
        self.run_command_on_instance(client,'echo "%s" >> ~/.ssh/authorized_keys' % key_pub)
        self.run_command_on_instance(client,'chmod 644 ~/.ssh/authorized_keys')
        self.run_command_on_instance(client,'chmod 700 ~/.ssh/')

        client.close()

    def __update_authorized_keys(self, key_file):
        import os

        key_pub = privateToPublicKey(key_file)
        ak = open(os.path.expanduser('~/.ssh/authorized_keys'), 'a')
        ak.write(key_pub)
        ak.close()


    def __ssh_client( self, key_file ):
        '''
        get a paramiko SSH client - i.e. a session with a SSH server
        if a path to SSH key is specified, the client will connect
        using that key.

        Note that the SSH client must be closed by the function that uses it

        :param key_file: path to the SSH key file needed to connect
        '''

        client = SSHClient( )
        client.set_missing_host_key_policy( paramiko.AutoAddPolicy() )
        #lient.save_host_keys('/home/fabio/hostkeys')

        try:
            client.connect( hostname=self.ip_address,
                        username=self.admin_account( ),
                        key_filename=key_file,
                        timeout=5.0 )

        except paramiko.SSHException as e:
            log.error("Error while connecting to the instance using SSH: %s", e)
            raise
        except paramiko.BadHostKeyException as b:
            log.error("Error while connecting to the instance using SSH: %s", b)
            raise
        except paramiko.AuthenticationException as u:
            log.error("Error while connecting to the instance using SSH: %s", u)
            raise

        return client


    def __ssh_args( self, user, command ):
        if user is None: user = self.default_account( )
        # Using host name instead of IP allows for more descriptive known_hosts entries and
        # enables using wildcards like *.compute.amazonaws.com Host entries in ~/.ssh/config.
        return [ 'ssh', '%s@%s' % (user, self.host_name), '-A' ] + command


    def ssh( self, user=None, command=None ):
        '''
        execute command through ssh
        '''
        if command is None: command = [ ]
        status = subprocess32.call( self.__ssh_args( user, command ) )
        # According to ssh(1), SSH returns the status code of the remote process or 255 if
        # something else went wrong. Python exits with status 1 if an uncaught exception is
        # thrown. Since this is also the default status code that most other programs return on
        # failure, there is no easy way to distinguish between failures in programs run remotely
        # by ssh and something being wrong in this app.
        if status == 255:
            raise RuntimeError( 'ssh failed' )
        return status


    # lunch rsync command
    def rsync( self, args, user=None, ssh_opts=None ):
        ssh_args = self.__ssh_args( user, [ ] )
        if ssh_opts:
            ssh_args.append( ssh_opts )
        subprocess32.check_call( [ 'rsync', '-e', ' '.join( ssh_args ) ] + args )



    def __optimize_spot_bid(self, instance_type, bid_price):
        '''
        There is a high variability of prices between zones and between instance types;
        Bidding high above an observed spot price leads to a large cost increase without
        a significant decrease in a computation time.
        There are data transfer fees when moving data between zones! If we write out
        results to S3, no actual data transfer between zones.

        Pricing is to maximize revenue, given the user demand, in a supposed (but
        realistic) infinite resource availability.


        Static bidding strategy VS dynamic bidding strategies that adjust bid prices
        according to application's execution requirements and market prices.

        Static bidding example: bid with one quarter of the on-demand price;
        bid with 25% more of the minimum price in the spot pricing history

        Dynamic bidding example: bid according to the probability distribution
        of all the market prices existed in the spot pricing history, and the
        remaining deadline at the beginning of each instance hour;


        The  optimal  bidding  price  only  depends  on a  job's  sensitivity
        to  delay.

        The value of a compute job is obviously of relevance when it comes to
        determining what a user is willing to pay to have it executed.

        According to the paper on Bidding Strategies, "bidding with 25% of on-demand price
        gives most balanced performance for scientific workflows"
        (Experimental Study of Bidding Strategies for Scientific Workflows using AWS
        Spot Instances)

        :return: the best stable zone AND an optimal bid price? (WHAT IS AN OPTIMAL BID PRICE?)
        '''

        # zones over the bid price and under bid price
        markets_under_bid, markets_over_bid = [], []
        zone_hists, prices = [], []

        log.info("Optimising bid price and placement for the spot request...")

        spot_hist = self.__get_spot_history(instance_type)
        zones = self.env.ec2client.describe_availability_zones()

        def check_spot_prices(spot_hist):
            sh = [round(float(pr['SpotPrice']),4) for pr in spot_hist]
            if sh:
                avg = mean(sh)
                smaller = min(sh)
                return (smaller, round(avg,4))


        # find best stable zone for placing spot instances
        for zone in zones['AvailabilityZones']:
            resp = [zone_hists.append(zh) for zh in spot_hist if zh['AvailabilityZone'] == zone['ZoneName']]

            recent_price = 0.0
            if zone_hists:
                prices = [round(float(hp['SpotPrice']),4) for hp in zone_hists]
                #prices = map(float, prices)
                price_dev = std_dev(prices)
                recent_price = round(float(zone_hists[0]['SpotPrice']),4)
                best_zone = BestAvZone(name=zone['ZoneName'], price_deviation=price_dev)
            else:
                price_dev, recent_price = 0.0, bid_price
                best_zone = BestAvZone(name=zone['ZoneName'], price_deviation=price_dev)

            # if False on first, else on second
            (markets_over_bid, markets_under_bid)[recent_price < bid_price].append(best_zone)

        stable_zone = min(markets_under_bid or markets_over_bid,
                          key=attrgetter('price_deviation')).name

        # Check spot history and deduce if it is a reasonable spot price
        sm, avg = check_spot_prices(spot_hist)
        if bid_price > (avg*2.0):
            log.info("Bid price is twice than the average spot price of the last week:\n"
                        " - YOURS: %s --> AVG: %s)", str(bid_price), str(avg))
            bid_price = avg + ((25/100)*avg)
            log.info("Bidding with 25%% more than the average: %s", str(bid_price))

        if bid_price <= sm:
            bid_price = sm + ((25/100)*sm)
            log.info("Bid price is %s, i.e. 25%% more than the minimum in spot pricing history",
                        str(bid_price))

        log.info("Spot request placed in %s at $s$", stable_zone, bid_price)
        return (stable_zone, bid_price)


    def __get_spot_history(self, instance_type):
        '''
        Returns the spot price history of the last week, for a given
        instance type, starting from the most recent.
        '''

        one_week_ago = datetime.datetime.utcnow() - datetime.timedelta(days=7)
        spot_hist = self.env.ec2client.describe_spot_price_history(
            EndTime = datetime.datetime.utcnow(),
            StartTime=one_week_ago,
            InstanceTypes=[instance_type],
            ProductDescriptions=['Linux/UNIX']
            )

        sh = sorted(spot_hist['SpotPriceHistory'], key=itemgetter("Timestamp"), reverse=True)
        return sh


    def __get_best_instance_type_fitting_budget(self, budget):
        '''
        Return the instance type and AvZone that are more likely to
        fit with the proposed budget

        TODO: budget means a total budget for the whole infrastructure?
              if so, we ha ve partition it, and save some for the instances,
              but we should estimate an total running time for the WF?
              Or maybe just pick the instances that can better accommodate
              the highest computing requirements expressed in the WF
              description parsed by CWL.
        '''
        one_week_ago = datetime.datetime.utcnow() - datetime.timedelta(days=7)
        spot_hist = self.env.ec2client.describe_spot_price_history(
            EndTime = datetime.datetime.utcnow(),
            StartTime=one_week_ago,
            ProductDescriptions=['Linux/UNIX']
            )

        sh = sorted(spot_hist['SpotPriceHistory'], key=itemgetter("Timestamp"), reverse=True)
        return sh


    def __fix_spot( self, instance_type, bid=None):
        '''
        Returns the best zone where the spot instances can be placed, and the
        best spot price.

        If an initial bid price is not specified, it takes the highest recent
        spot price for the specified instance type
        '''
        if not bid:
            _hist = self.__get_spot_history(instance_type)
            _prices = [round(float(pr['SpotPrice']),4) for pr in _hist]
            mx =  max(_prices)
            bid = mx - ((25/100)*mx) # 25% less than the highest price

        best_zone,best_bid = self.__optimize_spot_bid( instance_type, bid )
        return BestAvZone(name=best_zone,price_deviation=best_bid)


    def __get_image( self, image_ref=None ):
        if image_ref is None:
            image = self._base_image( )
        else:
            image = self.env.ec2client.describe_images(ImageIds=image_ref)
        return image




