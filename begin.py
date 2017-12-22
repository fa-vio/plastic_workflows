import logging
import os

from provisioning.aws.services import Service
from provisioning.aws.aws_engines import UbuntuSystemD

log = logging.getLogger('cloud_provision')
log.setLevel(logging.INFO)
console = logging.StreamHandler()
log.addHandler(console)


def main():

    # assumes file ~/.aws/credentials contains access_key_id and secrect_key_id fields
    # user must specify av_zone
    myenv = Service('eu-west-1a')

    myinst = UbuntuSystemD(myenv)

    image = myinst.base_image()
    specs = myinst.prepare(keyName='wed-key1',
                           image_id=image,
                           num_instances=1,
                           spot_auto_tune=True
                           )

    print specs

    insts = myinst.create(arguments=specs,
                          terminate_on_error=True,
                          user_data=os.getcwd()+'/init_scripts/init_script_for_mesos_slave_ubuntu16'
                          )

    for i in insts:
        i.load()
        print i.id
        print i.public_ip_address
        print i.tags


#     filters = [{'Name': 'virtualization-type', 'Values':['hvm']},
#                {'Name': 'architecture', 'Values':['x86_64']},
#                {'Name': 'image-type', 'Values':['machine']},
#                {'Name': 'root-device-type', 'Values':['ebs']},
#                {'Name': 'name', 'Values':['ubuntu*']}
#                ]



    #resp = create_ec2_instances(myenv, count=2)
    #for ips in resp:
    #waitForOpenPort('54.154.148.187')

    #terminate_instance(myenv)

if __name__ == "__main__":
    main()
