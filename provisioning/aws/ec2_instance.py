
import errno
import time
import datetime
import logging

from operator import attrgetter, itemgetter

from . import BestAvZone, defaultType, defaultImage

from utils import UserError, mean, std_dev
from provisioning.aws import ec2_instance_types

log = logging.getLogger( "cloud_provision" )


class UnexpectedResourceState( Exception ):
    def __init__( self, resource, to_state, state ):
        super( UnexpectedResourceState, self ).__init__(
            "Expected state of %s to be '%s' but got '%s'" %
            (resource, to_state, state)
        )


# Check whether we are running on EC2 or not
def running_on_ec2( ):
    try:
        with open( '/sys/hypervisor/uuid' ) as f:
            return f.read( 3 ) == 'ec2'
    except IOError as e:
        if e.errno == errno.ENOENT:
            return False
        else:
            raise


# wait for transition to happen
def wait_transition( ec2resource, from_states, to_state, state_getter=attrgetter( 'state' ) ):
    """
    Wait until the specified EC2 'resource' (instance, image, volume, ...) moves from any
    of the given 'from' states to the specified 'to' state. If the instance is found in a state
    other that the to state or any of the from states, an exception will be thrown.

    :param resource: the EC2 resource to monitor
    :param from_states: a set of states that the resource is expected to be in before
                        the transition occurs
    :param to_state: the state of the resource when this method returns
    """
    state = state_getter( ec2resource )
    while state in from_states:
        time.sleep( 5 )
#         for attempt in retry_ec2( ):
#             with attempt:
#                 resource.update( validate=True )
        state = state_getter( ec2resource )

    if state != to_state:
        raise UnexpectedResourceState( ec2resource, to_state, state )


def create_ec2_instances(env, imageId=defaultImage, count=1, instType=defaultType,
                         secGroup=None, keyName=None, Placement=None,
                         subnet=None, usr_data=None, **other_opts):
    """
    Requests boto3 API to create EC2 on_demand instance(s).
    Default instance type is 'm3.medium', equipped with 1 core, 3.75GB of RAM,
    1 SSD ephemeral disk with 4GB.
    Default image is a Ubuntu16 @ eu-west-1

    :param env: a 'Service' object, that encapsulates AWS-specific services and
                provides access to user settings.
    :rtype: public ips of created instances
    """

    if env is None:
        raise UserError("Cannot start any AWS service without an AWS object")

    log.info("Creating %s instance(s) using the key '%s'", str(count), keyName)
    instances = env.ec2.create_instances(
        ImageId=imageId,
        MinCount=1,
        MaxCount=count,
        InstanceType=instType,
        KeyName=keyName,
        SecurityGroupIds=secGroup,
        UserData=usr_data, #open("/shelf/fabio/lilWS/cloud_provision/init_scripts/sample_script.sh").read(),
        **other_opts
    )

    #thread = threading.Thread(target=wait_running, args=(instances))
    #thread.start()

    log.info("Instance(s) created. "
             "Waiting for instances to be in running state...")
    for inst in instances:
        inst.wait_until_running()
        inst.load()
        log.info("[%s] Instance %s is running.", inst.public_ip_address, inst.id)

    #thread.join()

#     pub_ips = []
#     for inst in instances:
#         inst.load()
#         pub_ips.append(inst.public_ip_address)

    return instances


def create_ec2_spot_instances(spot_price, env, imageId=defaultImage, count=1, secGroup=None,
                              instType=defaultType, keyName=None, Placement=None,
                              subnet=None, usr_data=None, **other_opts):
    """
    Requests boto3 API to create EC2 spotinstance(s)

    :rtype: list(ec2.Instance)
    """

    if env is None:
        raise UserError("Cannot start any AWS service without an AWS object")

    if not keyName:
        keyName = env.get_key_pair()
    else:
        keyName = env.get_key_pair(keyName)

    ebsopt = True if ec2_instance_types[instType].EBS_optimized else False

    spot_response = env.ec2client.request_spot_instances(
    SpotPrice = str(spot_price),
    InstanceCount = count,
    LaunchSpecification = {
        'ImageId': imageId,
        'KeyName': keyName[0],
        'InstanceType': instType,
        'SecurityGroupIds': secGroup,
        'Placement': {
            'AvailabilityZone': Placement.AvailabilityZone,
            'GroupName' : Placement.GroupName
        },
        'BlockDeviceMappings': [{
            'DeviceName': '/dev/sdg',
            'Ebs': {
                'VolumeSize': 12,
                'DeleteOnTermination': True,
                'VolumeType': 'gp2',
                # 'Iops': 300,
                'Encrypted': False
                }
        }],
        'EbsOptimized': ebsopt,
        'Monitoring': {
            'Enabled': False
        },
        'SubnetId' : subnet,
        'UserData' : usr_data
    })

    return spot_response


def terminate_instance(env, instance_id=None):
    '''
    Terminates the instance specified with the specified ID.
    If no ID is given, stops all instances without termination, and
    prints a warning message.

    :return: the id of the terminated instance or the ids of the stopped
             instances
    '''

    if not env:
        raise UserError("Cannot start any AWS service without an AWS object")

    if not instance_id:
        log.info("No instance ID provided: stopping all instances. "
                 "It will be possible to restart them.")
        instances = env.ec2.instance.all()
        stopped = []
        for inst in instances:
            resp = inst.stop()
            log.info("Stopping instance %s" % resp['StoppingInstances'][0]['InstanceId'])
            stopped.append(resp['StoppingInstances'][0]['InstanceId'])
        return stopped
    else:
        log.info("Terminating instance %s" % instance_id)
        resp = env.ec2.Instance(instance_id).terminate()
        return resp['StoppingInstances'][0]['InstanceId']

def wait_spot_requests_fullfilled( env, spot_req_id ):
    """
    Wait until all spot requests are fulfilled.

    :param env: The AWS environment to work on, i.e., a Service object

    :rtype: dictionary containing the status of all spot instances requests
    """
    count_tries = 0
    log.warning("Waiting for spot request to be fullfilled...")
    while count_tries < 10:
        response = env.ec2client.describe_spot_instance_requests(
            Filters=[{'Name':'spot-instance-request-id','Values':[spot_req_id]}]
        )
        resp = response["SpotInstanceRequests"]
        for r in resp:
            if r['State'] == 'failed':
                msg = "Spot request failed due to %s:\n%s" % (r['Status']['Code'], r['Status']['Message'])
                raise UserError(message=msg)

            if r['State'] == 'open':
                log.warning("...request pending: %s", r['Status']['Code'])
                time.sleep(5)
                count_tries += 1
                continue

            if r['State'] == 'active' and r['Status']['Code'] == 'fulfilled':
                log.info("Request %s!", r['Status']['Code'])
                log.info("%s!", r['Status']['Message'])
                return r['InstanceId']

    #env.ec2client.

def __awsBillingInterval(instances):
    """
    Takes a list of EC2 instances and determines its current billing cycle.
    This function assumes a hourly billing cycle.

    :param instances: a list of (boto3) EC2 instances
    :return: the remaining billing interval for the current instance(s)
    """
    launch_timings = []
    for inst in instances:
        inst.load()
        launch_time = inst.launch_time
        launch_time = launch_time.replace(tzinfo=None)
        now = datetime.datetime.utcnow()
        delta = now - launch_time
        launch_timings.append(delta.total_seconds() / 3600.0 % 1.0)

    avg_launch = mean(launch_timings)

    return 1.0 - round(avg_launch, 3)


def __optimize_spot_bid(env, instance_type, bid_price):
    '''
    There is a high variability of prices between zones and between instance types;
    Bidding high above an observed spot price leads to a large cost increase without
    a significant decrease in a computation time.
    There are data transfer fees when moving data between zones! If we write out
    results to S3, no actual data transfer between zones.

    Pricing is to maximize revenue, given the user demand, in a supposed (but
    realistic) infinite resource availability.

    Static bidding strategy VS dynamic bidding strategies that adjusts bid prices
    according to application's execution requirements and market prices.

    The  optimal  bidding  price  only  depends  on a  job's  sensitivity  to  delay.

    The value of a compute job is obviously of relevance when it comes to
    determining what a user is willing to pay to have it executed.

    According to the paper on Bidding Strategies, "bidding with 25% of on-demand price
    gives most balanced performance for scientific workflows"
    (Experimental Study of Bidding Strategies for Scientific Workflows using AWS
    Spot Instances)

    :rtype: the best stable zone AND an optimal bid price? (WHAT IS AN OPTIMAL BID PRICE?)
    '''

    # zones over the bid price and under bid price
    markets_under_bid, markets_over_bid = [], []
    zone_hists, prices = [], []

    spot_hist = __get_spot_history(env, instance_type)
    zones = env.ec2client.describe_availability_zones()

    # find best stable zone for placing spot instances
    for zone in zones['AvailabilityZones']:
        resp = [zone_hists.append(zh) for zh in spot_hist if zh['AvailabilityZone'] == zone['ZoneName']]

        if zone_hists:
            prices = [hp['SpotPrice'] for hp in zone_hists]
            prices = map(float, prices)
            price_dev = std_dev(prices)
            recent_price = float(zone_hists[0]['SpotPrice'])
        else:
            price_dev, recent_price = 0.0, bid_price

        best_zone = BestAvZone(name=zone['ZoneName'], price_deviation=price_dev)
        (markets_over_bid, markets_under_bid)[recent_price < bid_price].append(best_zone)


    stable_zone = min(markets_under_bid or markets_over_bid,
                      key=attrgetter('price_deviation')).name

    # Check spot history and deduce if it is a reasonable spot price
    sh = [pr['SpotPrice'] for pr in spot_hist]
    sh = [round(float(i),2) for i in sh]
    if sh:
        avg = mean(sh)
        if bid_price > avg*2:
            log.warning("Bid price is twice the average spot price in this region for the last week. "
                    "(YOURS: %s; AVG: %s)\n"
                    "Halving it!" )
            bid_price /= 2

    return (stable_zone,bid_price)


def __check_bid(env, inst_type, spot_price):
    '''
    Check spot history and deduce a reasonable spot price
    '''

    spot_hist = __get_spot_history(env, inst_type)
    avg = mean(spot_hist)

    if spot_price > avg*2:
        log.warning("Bid price is twice the average spot price of the last week.\n"
                    "YOURS: %s; AVG: %s", spot_price, avg )



def __get_spot_history(env, instance_type):
    '''
    Returns the spot price history of the last week, for a given
    instance type, starting from the most recent.
    '''

    one_week_ago = datetime.datetime.utcnow() - datetime.timedelta(days=7)
    spot_hist = env.ec2client.describe_spot_price_history(
        EndTime = datetime.datetime.utcnow(),
        StartTime=one_week_ago,
        InstanceTypes=[instance_type],
        ProductDescriptions=['Linux/UNIX']
    )

    sh = sorted(spot_hist['SpotPriceHistory'], key=itemgetter("Timestamp"), reverse=True)
    return sh



# Not currently used: functions needed to download and
# parse AWS Price List API response, so that we can get
# on-demand prices and other stuff
#    In order to check the on-demand price, we need to download and parse a 170MB file
#    using AWS Price List API. E.G.:
#        response = requests.get(
#        "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json"
#        )
#
#    then filter the products by, for example, shared tenancy and linux instances:
def lambda_handler():
    ec2_url = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json"
    offer = download_offer(ec2_url)
    prices = extract_prices(offer)
    #upload_prices(prices)

    import pprint

    pprint.pprint(prices, indent=4)


def download_offer(url):
    import  requests, json

    response = requests.get(url)
    return json.loads(response.text)


def filter_products(products):
    filtered = []

    # Only interested in shared tenancy, linux instances
    for sku, product in products:
        a = product['attributes']
        if not ('locationType' in a and
                'location' in a and
                'tenancy' in a and
                a['tenancy'] == "Shared" and
                a['locationType'] == 'AWS Region' and
                a['operatingSystem'] == 'Linux'):
            continue

        a['sku'] = sku
        filtered.append(a)

    return filtered


def extract_prices(offer):
    terms = offer['terms']
    products = offer['products'].items()

    instances = {}
    for a in filter_products(products):
        term = terms['OnDemand'][a['sku']].items()[0][1]
        cost = [float(term['priceDimensions'].items()[0][1]['pricePerUnit']['USD'])]

        info = {"type" : a['instanceType'], "vcpu" : a['vcpu'],
                "memory" : a['memory'].split(" ")[0], "cost" : cost}

        if not a['location'] in instances:
            instances[a['location']] = []

        instances[a['location']].append(info)

    return {'created': time.strftime("%c"), 'published': offer['publicationDate'],
            'instances': instances}
