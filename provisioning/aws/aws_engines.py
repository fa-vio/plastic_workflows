
import logging
import contextlib
import urllib2
import csv

from StringIO import StringIO
from fabric.operations import sudo, put

from provisioning.aws.engine import Engine, fabric_task

BASE_URL = 'http://cloud-images.ubuntu.com'
log = logging.getLogger( "aws_provision" )

def findLatestUbuntuImage(release, this_region, virtualization_type):
    class TemplateDict( dict ):
        def matches( self, other ):
            return all( v == other.get( k ) for k, v in self.iteritems( ) )

    template = TemplateDict( release=release, purpose='server', release_type='release',
                                      storage_type='ebs-ssd', arch='amd64', region=this_region,
                                      hypervisor=virtualization_type )
    url = '%s/query/%s/server/released.current.txt' % (BASE_URL, release)
    matches = [ ]
    with contextlib.closing( urllib2.urlopen( url ) ) as stream:
        images = csv.DictReader(stream,
                                fieldnames=[
                                    'release', 'purpose', 'release_type', 'release_date',
                                    'storage_type', 'arch', 'region', 'ami_id', 'aki_id',
                                    'unused', 'hypervisor' ],
                                    delimiter='\t' )
        for image in images:
            if template.matches( image ):
                matches.append( image )

    if len( matches ) < 1:
        raise RuntimeError(
                "Can't find Ubuntu AMI for release %s and virtualization type %s in region %s" % (
                    release, virtualization_type, this_region) )

    if len( matches ) > 1:
            raise RuntimeError( 'More than one matching image: %s' % matches )

    image_info = matches[ 0 ]
    return image_info[ 'ami_id' ]



class UpstartEngine( Engine):
    """
    An engine that uses upstart (i.e., /etc/init/ folder's scripts)
    """

    @fabric_task
    def _register_init_script( self, name, script ):
        path = '/etc/init/%s.conf' % name
        put( local_path=StringIO( script ), remote_path=path, use_sudo=True )
        sudo( "chown root:root '%s'" % path )

    @fabric_task
    def _run_init_script( self, name, command='start' ):
        sudo( "service %s %s" % ( name, command ) )


class SystemdEngine( Engine ):
    """
    An engine that supports Systemd init scripts.
    """

    @fabric_task
    def _register_init_script( self, name, script ):
        path = '/lib/systemd/system/%s.service' % name
        put( local_path=StringIO( script ), remote_path=path, use_sudo=True )
        sudo( "chown root:root '%s'" % path )

    @fabric_task
    def _run_init_script( self, name, command='start' ):
        sudo( 'systemctl %s %s' % ( command, name ) )


class UbuntuUpstart( UpstartEngine ) :
    """
    An engine for EC2 instances running Ubuntu AMIs
    """

    pkg_mngr = 'apt-get -q -y'

    def this_release( self ):
        amid = findLatestUbuntuImage(release='trusty',
                                       this_region=self.env.region,
                                       virtualization_type='hvm')
        self.release_info = self.Release( codename='trusty', version='14.04', ami=amid )
        return self.release_info

    def admin_account( self ):
        return 'ubuntu'

    @fabric_task
    def _upgrade_installed_packages( self ):
        sudo( '%s upgrade' % self.pkg_mngr )

    @fabric_task
    def _install_packages( self, packages ):
        packages = " ".join( packages )
        sudo( '%s install %s' % (self.pkg_mngr, packages) )

    def _ssh_service_name( self ):
        return 'ssh'

    def base_image( self ):
        this_release = self.this_release()
        return this_release.ami

    def setup(self, **kwargs):
        # TODO: use this to initial setup of an instance
        # maybe better use user_data
        pass



class UbuntuSystemD( SystemdEngine ) :
    """
    An engine for EC2 instances running Ubuntu AMIs
    """

    pkg_mngr = 'apt -q -y'

    def this_release( self ):
        amid = findLatestUbuntuImage(release='xenial',
                                       this_region=self.env.region,
                                       virtualization_type='hvm')
        self.release_info = self.Release( codename='xenial', version='16.04', ami=amid )
        return self.release_info

    def admin_account( self ):
        return 'ubuntu'

    @fabric_task
    def _upgrade_installed_packages( self ):
        sudo( '%s upgrade' % self.pkg_mngr )

    @fabric_task
    def _install_packages( self, packages ):
        packages = " ".join( packages )
        sudo( '%s install %s' % (self.pkg_mngr, packages) )

    def _ssh_service_name( self ):
        return 'ssh'

    def base_image( self ):
        this_release = self.this_release()
        return this_release.ami

    def setup(self, **kwargs):
        # TODO: use this to initial setup of an instance
        # maybe better use user_data
        pass

