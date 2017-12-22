import boto3
import re
import logging
import time
import datetime
import os
import stat

from botocore.exceptions import ClientError
from utils import randomizeID, UserError, to_aws_name, uniqueList

log = logging.getLogger( "aws_provision" )

class Service( object ):
    """
    A context manager class that encapsulates AWS-specific services and
    settings used in this project.
    It basically handles the connection to user's AWS accounts, and makes
    most commonly used services (EC2, S3, etc.) available.

    The only required parameter is an AWS availability zone (i.e. eu-west-1a')
    needed to create an AWS session, that stores configuration state and
    allows to create service clients and resources
    """

    availability_zone_re = re.compile( r'^([a-z]{2}-[a-z]+-[1-9][0-9]*)([a-z])$' )
    name_prefix_re = re.compile( r'^(/([0-9a-z][0-9a-z._-]*))*' )
    name_re = re.compile( name_prefix_re.pattern + '/?$' )

    def __init__( self, availability_zone ):

        super( Service, self ).__init__( )

        # these objects represent the basic blocks of AWS, and will be deployed
        # using new boto3 syntax
        self.__session = None
        self.__ec2 = None
        self.__iam = None
        self.__vpc = None
        self.__s3  = None
        self.__sns = None
        self.__sqs = None
        self.__key_pairs = []

        # this session's availability zone
        self.availability_zone = availability_zone

        m = self.availability_zone_re.match( availability_zone )
        if not m:
            raise ValueError( "Can't extract region from availability zone '%s'"
                              % availability_zone )

        self.aws_region = m.group( 1 )

        # if credentials are set up, create custom Session
        # raise an error otherwise, then exit
        self.__resolveMe()
        self.__updateMyKeyPairs()


    # make this object available for use as a context manager
    # i.e., to be used as "with Service('eu-east-1') as service:"
    def __enter__( self ):
        return self

    def __exit__( self ):
        # TODO: should close all active connections
        self.close( )

    def close( self ):
        # it seems that in boto3 there is no need to close and clean connections, it
        # handles it automatically.
        log.info("Closing session and connections...")
        time.sleep(3)
        log.info("Session closed successfully")


    @property
    def session( self ):
        """
        A session stores the configuration state, and allows to create
        service clients and resources

        :rtype: Session type
        """
        # TODO: should never be None, checked at init time
        if self.__session is None:
            #self.__session = boto3.session.Session( region_name=self.region )
            self.__resolveMe()
        return self.__session

    @property
    def zone( self ):
        """
        The availability zone where this session has been initiated
        """
        return self.availability_zone

    @property
    def region(self):
        """
        The AWS region where this session has been initiated
        """
        return self.aws_region

    @property
    def iam( self ):
        """
        :rtype: IAM resource
        """
        if self.__iam is None:
            self.__iam = self.session.resource('iam', region_name=self.region)
        return self.__iam

    @property
    def ec2( self ):
        """
        :rtype: EC2 resource
        """
        if self.__ec2 is None:
            self.__ec2 = self.session.resource('ec2')
        return self.__ec2

    @property
    def ec2client( self ):
        """
        :rtype: EC2 client
        """
        return self.session.client('ec2')

    # VPC is a resource of EC2
    @property
    def vpc( self ):
        """
        :rtype: VPC resource
        """
        if self.__vpc is None:
            self.__vpc = self.__findDefaultVPC()
        return self.__vpc

    @property
    def s3( self ):
        """
        :rtype: S3 resource
        """
        if self.__s3 is None:
            self.__s3 = self.session.resource('s3')
        return self.__s3

    @property
    def sns( self ):
        """
        :rtype: SNS resource
        """
        if self.__sns is None:
            self.__sns = self.session.resource('sns')
        return self.__sns

    @property
    def sqs( self ):
        """
        :rtype: SQS resource
        """
        if self.__sqs is None:
            self.__sqs = self.session.resource('sqs')
        return self.__sqs

    @property
    def accountId( self ):
        """
        :rtype: string: current IAM user unique identifier
        """
        current_user = self.iam.CurrentUser()
        current_user.load()
        user_id = current_user.user_id

        return user_id

    @property
    def userName( self ):
        """
        :rtype: string: current IAM user friendly name
        """
        current_user = self.iam.CurrentUser()
        current_user.load()
        user_name = current_user.user_name

        return user_name


    @property
    def s3BucketName( self ):
        """
        :rtype: string: a name for an S3 bucket
        """
        stamp = str(datetime.datetime.now())
        stamp = stamp.split('.')[0].replace('-', '').replace(':', '').replace(' ', '-')
        #stamp = stamp.replace('0', 'o').replace('1', 'i').replace('4', 'a')
        return self.userName + '-' + stamp


    @property
    def s3Location( self ):
        if self.region == 'us-east-1':
            return ''
        else:
            return self.region


    @property
    def keyPairs( self ):
        '''
        :rtype: list: an array containing names of existing keypairs
                for this account (does not check if list is empty)
        '''
        return self.__key_pairs


    def get_key_pair(self, keyName=None):
        '''
        Obtains a KeyPair name.
        If a key name is given, searches for the key among existing keys for
        the current user. If no key exists, or no key with such name is found,
        a new key with the given name is created.

        If no key name is specified, returns the first available key, if any,
        or creates a new key and returns its name.
        The key will have a name of the form:
            userName + '_KP_' + AVzone + '_' + uniqueID

        :return: a tuple containing key pair name and path to stored private key
        '''
        if keyName:
            if self.__hasKeyPairs(): # look among known key-pairs
                match = [k for k in self.keyPairs if keyName in k]
                if match:
                    return self.__findSshKeyInBucket(keyName)

                    # TODO: look for the private key, needed to ssh
                else: # create a new key with given name
                    return self.__createKeyPair(keyName=keyName)
            else:
                return self.__createKeyPair(keyName=keyName)

        # no key name given
        if self.__hasKeyPairs():
            return self.__findSshKeyInBucket(self.keyPairs[0]) # use the first one
            # TODO: look for the private key, needed to ssh
        else:
            return self.__createKeyPair()


    def __findSshKeyInBucket(self, keyName, buck_name=None):
        '''
        If the key is in our bucket, we can download it (private key)
        and store it locally.
        Key permission are modified into 0600

        Note that a smarter way would be to store public keys in the
        bucket, and inject it into the instances.

        :return: a tuple containing (keyName, path/to/private/key.pem)
        '''

        if not buck_name:
            buck_name = 'cloud-prov.services.keys-store'

        keyName = keyName+'.pem'
        resp = self.s3.meta.client.list_objects_v2(Bucket=buck_name)
        for it in resp['Contents']:
            if it['Key'] == keyName:
                key_path = os.getcwd()+'/key_in_use'
                if not os.path.isdir(key_path):
                    os.mkdir(key_path)

                self.s3.Bucket(buck_name).download_file(keyName, key_path+'/'+keyName)

                os.chmod(key_path+'/'+keyName, stat.S_IREAD | stat.S_IWRITE)
                return (keyName.split('.')[0], key_path+'/'+keyName)


    def __createKeyPair( self, keyName=None ):
        '''
        Creates an EC2 keypair for the current user.
        If no key name is provided, the key will have a name of the form:
            userName + '_KP_' + AVzone + '_' + uniqueID

        The key is stored in an S3 bucket, so that it can be downloaded
        in the future and used for SSH to the created instance(s).
        The key is also stored locally.

        :return: a string containing the key name
        '''
        stamp = str(datetime.datetime.now())
        uq = randomizeID(stamp,4)
        if not keyName:
            keyName = self.userName + '_KP_' + self.zone + '_' + uq

        try:
            key_pair = self.ec2.create_key_pair(KeyName=keyName)
        except ClientError as e:
            log.error("Received an error creating a Key Pair: %s", e, exc_info=True )

        self.__updateMyKeyPairs()

        path_to_key = self.__saveSshKey(keyName, key_pair)

        return (key_pair.key_name, path_to_key)


    def __hasKeyPairs(self):
        return len(self.__key_pairs) != 0


    # retrieve AWS KeyPairs stored for the current user
    def __updateMyKeyPairs( self ):
        '''
        retrieve AWS KeyPairs stored for the current user
        '''
        keys = self.ec2client.describe_key_pairs()

        for n in keys['KeyPairs']:
            self.__key_pairs.append(n['KeyName'])

        self.__key_pairs = uniqueList(self.__key_pairs)


    def __saveSshKey(self, keyName, keypair):
        '''
        Store the SSH private key in an AWS S3 bucket, using AES 256
        ServerSide Encryption

        :return: the local path/to/private/key.pem
        '''
        key_content = str(keypair.key_material)
        key_bucket = 'cloud-prov.services.keys-store'
        bucket = self.__createS3Bucket(name=key_bucket)
        self.s3.Object(bucket.name, keyName+'.pem').put(Body=key_content,ServerSideEncryption='AES256')

        key_path = os.getcwd()+'/key_in_use'
        if not os.path.isdir(key_path):
            os.mkdir(key_path)

        key_filename = key_path+'/'+keyName+'.pem'
        outfile = open(key_filename, 'w')
        outfile.write(key_content)
        os.chmod(key_filename, stat.S_IREAD | stat.S_IWRITE)
        return key_filename


    def __bucketExists(self, name ):
        exists = True

        try:
            self.s3.meta.client.head_bucket(Bucket=name)
        except ClientError as e:
            # If a client error is thrown, a 404 error is raised
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                exists = False

        return exists


    def __createS3Bucket( self, name=None ):
        '''
        Attempts to create an empty S3 bucket in current region.
        If a bucket with the given name exists, retrieves that bucket

        If no name is given, a name is generated using the following pattern:
            accountId + '-' + time_stamp
        A new bucket with the generated name is created

        :return: the created (o retrieved) bucket object.
        '''
        if not name:
            name = self.s3BucketName

        name = to_aws_name(name)
        print name
        if not self.__bucketExists(name):
            buck = self.s3.create_bucket(
                                Bucket=name,
                                CreateBucketConfiguration={
                                    'LocationConstraint':self.s3Location
                                }
            )
            log.info("Waiting for the bucket until exists    ...")
            buck.wait_until_exists()
        else:
            buck = self.s3.Bucket(name)

        return buck

    def __findDefaultVPC(self):
        for vpc in self.ec2.vpcs.all():
            vpc.load()
            if vpc.is_default:
                return vpc

        return None # never reached

    def __create_new_vpc(self):
        '''
        Create a Virtual Private Cloud for 512 hosts.
            Netmask:  255.255.254.0
            First IP: 192.168.100.0
            Last IP:  192.168.101.255
        '''
        return self.ec2.create_vpc(CidrBlock='192.168.100.0/23')


    def __resolveMe( self ):
        try:
            # create a new session with available credentials
            self.__session = boto3.session.Session( region_name=self.region )
        except Exception as _:
            raise UserError( "Can't determine current IAM user name. Be sure to put valid AWS "
                             "credentials in environment variables or in ~/.aws/credentials. "
                             "For details, refer to %s."
                             % 'http://boto3.readthedocs.io/en/latest/guide/configuration.html' )

    # TODO: handle notifications via SNS and SQS?
