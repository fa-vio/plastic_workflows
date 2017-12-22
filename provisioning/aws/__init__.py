
from collections import namedtuple

a_short_time = 5
a_long_time = 60 * 60

InstanceType = namedtuple( 'InstanceType', [
    'name',             # the API name of the instance type
    'cores',            # the number of cores
    'memory',           # RAM in GB
    'disks',            # the number of ephemeral (aka 'instance store') volumes
    'disk_type',        # the type of ephemeral volume
    'disk_capacity',    # the capacity of each ephemeral volume in GB
    'EBS_optimized'
] )

BestAvZone = namedtuple('BestAvZone', ['name', 'price_deviation'])


# some global default variables
ssd='ssd'

# default instance type: the minimum instance type that can be
# both preemptible and on-demand
defaultType = 'c3.large'

# Ubuntu16 @ eu-west-1 -
#TODO: this should be a custom image
defaultImage = 'ami-6d48500b'

# contains only instances that can be launched as preemptible requests
_ec2_instance_types = [
    # current generation instance types
    InstanceType( 'c3.large',    2, 3.75, 2, ssd,  16, False ),
    InstanceType( 'c3.xlarge',   4,  7.5, 2, ssd,  40, False ),
    InstanceType( 'c3.2xlarge',  8,   15, 2, ssd,  80, False ),
    InstanceType( 'c3.4xlarge', 16,   30, 2, ssd, 160, False ),

    InstanceType( 'c4.large',    2, 3.75, 0, None,  0, True ),
    InstanceType( 'c4.xlarge',   4,  7.5, 0, None,  0, True ),
    InstanceType( 'c4.2xlarge',  8,   15, 0, None,  0, True ),
    InstanceType( 'c4.4xlarge', 16,   30, 0, None,  0, True ),

    InstanceType( 'm3.medium',  1, 3.75, 1, ssd,  4, False ),
    InstanceType( 'm3.large',   2, 7.5,  1, ssd, 32, False ),
    InstanceType( 'm3.xlarge',  4,  15,  2, ssd, 40, False ),
    InstanceType( 'm3.2xlarge', 8,  30,  2, ssd, 80, False ),

    InstanceType( 'm4.large',     2,   8, 0, None, 0, True ),
    InstanceType( 'm4.xlarge',    4,  16, 0, None, 0, True ),
    InstanceType( 'm4.2xlarge',   8,  32, 0, None, 0, True ),
    InstanceType( 'm4.4xlarge',  16,  64, 0, None, 0, True ),

    InstanceType( 'g2.2xlarge',  8, 15, 1, ssd,  60, False ),
    InstanceType( 'g2.8xlarge', 32, 60, 2, ssd, 120, False ),

    InstanceType( 'g3.4xlarge', 16, 122, 0, None, 0, True ),
    InstanceType( 'g3.8xlarge', 32, 144, 0, None, 0, True ),

    InstanceType( 'r3.large',    2,    15, 1, ssd,  32, False ),
    InstanceType( 'r3.xlarge',   4,  30.5, 1, ssd,  80, False ),
    InstanceType( 'r3.2xlarge',  8,    61, 1, ssd, 160, False ),
    InstanceType( 'r3.4xlarge', 16,   122, 1, ssd, 320, False ),

    InstanceType( 'r4.large',    2, 15.25, 0, None, 0, True ),
    InstanceType( 'r4.xlarge',   4,  30.5, 0, None, 0, True ),
    InstanceType( 'r4.2xlarge',  8,    61, 0, None, 0, True ),
    InstanceType( 'r4.4xlarge', 16,   122, 0, None, 0, True )
]

ec2_instance_types = dict( (_.name, _) for _ in _ec2_instance_types )
