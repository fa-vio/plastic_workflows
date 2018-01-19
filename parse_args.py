import logging
import os
import re
import sys
import tempfile
import time
from argparse import ArgumentParser

# Python 3 compatibility imports
from six.moves import cPickle
from six import iteritems

from utils import parse_bytes, parse_set_env_var, print_bytes


log = logging.getLogger('cloud_provision')


class Params(object):
    """
    This object represents the configuration for a workflow: uses argparse to
    parse command-line parameters and set-up a working environment for the
    workflow to run
    """

    def __init__(self):
        self.workflowID = None
        """
        Workflow unique identifier. It is useful to distinguish between two
        consecutive workflows which may have the same name (e.g., same WF run
        twice, on different data as well as on the very same data).
        """

        self.workflowAttemptNumber = None
        self.storage = None
        self.workingDir = None
        self.stats = False

        self.clean = None
        self.cleanWorkDir = None
        self.clusterStats = None

        #Restarting the workflow options
        self.restart = False

        #Batch system options
        self.batchSystem = "singleMachine"
        self.scale = 1
        self.mesosMasterAddress = 'localhost:5050'
        self.environment = {}

        #Plasticity options
        self.provisioner = 'aws'
        self.nodeType = None
        self.minNodes = 1
        self.maxNodes = 10
        self.preemptableNodeType = None
        self.preemptableNodeOptions = None
        self.minPreemptableNodes = 0
        self.maxPreemptableNodes = 0

        # default values for bin-packing first-fit decreasing
        #self.alphaPacking = 0.8
        #self.betaInertia = 1.2
        self.scaleInterval = 10
        self.preemptableCompensation = 0.0

        #Resource requirements
        self.defaultMemory = 2147483648
        self.defaultCores = 1
        self.defaultDisk = 2147483648
        self.defaultPreemptable = False
        self.maxCores = sys.maxint
        self.maxMemory = sys.maxint
        self.maxDisk = sys.maxint

        #Misc
        self.maxLogFileSize = 64000
        self.useAsync = True


    def setOptions(self, option):
        """
        Creates a config object from the option object.
        """

        def setOption(var, parseFunc=None, checkFunc=None):
            #If option object has the "var" specified
            #then set the "var" attribute to this value
            x = getattr(option, var, None)
            if x is not None:
                if parseFunc is not None:
                    x = parseFunc(x)
                if checkFunc is not None:
                    try:
                        checkFunc(x)
                    except AssertionError:
                        raise RuntimeError("The %s option has an invalid value: %s"
                                           % (var, x))
                setattr(self, var, x)

        # Function to parse integer from string expressed in different formats
        h2b = lambda x : parse_bytes(str(x))

        def _check_int(val, maxValue=sys.maxint):
            # Check if a given value is in the given half-open interval
            assert isinstance(val, int) and isinstance(maxValue, int)
            return lambda x: val <= x < maxValue

        def _check_float(val, maxValue=None):
            # Check if a given float is in the given half-open interval
            assert isinstance(val, float)
            if maxValue is None:
                return lambda x: val <= x
            else:
                assert isinstance(maxValue, float)
                return lambda x: val <= x < maxValue

        def parseStorageLoc(loc):
            if loc[0] in '/.' or ':' not in loc:
                return 'file:' + os.path.abspath(loc)
            else:
                try:
                    name, rest = loc.split(':', 1)
                except ValueError:
                    raise RuntimeError('Invalid storage location.')
                else:
                    if name == 'file':
                        return 'file:' + os.path.abspath(rest)
                    else:
                        return loc

        #Core option
        setOption("storage", parseFunc=parseStorageLoc)

        #TODO: LOG LEVEL STRING
        setOption("workingDir")
        if self.workingDir is not None:
            self.workingDir = os.path.abspath(self.workingDir)
            if not os.path.exists(self.workingDir):
                raise RuntimeError("The path provided to --workingDir (%s) does not exist."
                                   % self.workingDir)
        setOption("stats")
        setOption("cleanWorkDir")
        setOption("clean")
        if self.stats:
            if self.clean != "never" and self.clean is not None:
                raise RuntimeError("Clean flag is set to %s "
                                   "despite the stats flag requiring "
                                   "the storage to be intact at the end of the run. "
                                   "Set clean to \'never\'" % self.clean)
            self.clean = "never"
        elif self.clean is None:
            self.clean = "onSuccess"
        setOption('clusterStats')

        #Restarting the workflow option
        setOption("restart")

        #Batch system option
        setOption("batchSystem")
        setOption("scale", float, _check_float(0.0))
        setOption("mesosMasterAddress")

        setOption("environment", parse_set_env_var)

        #Plasticity option
        setOption("provisioner")
        setOption("nodeType")
        setOption("minNodes", int)
        setOption("maxNodes", int)
        setOption("preemptableNodeType")
        setOption("preemptableNodeOptions")
        setOption("minPreemptableNodes", int)
        setOption("maxPreemptableNodes", int)

        # F.F. bin-packing options
#         setOption("betaInertia", float)
        setOption("scaleInterval", float)
        setOption("preemptableCompensation", float)
        if 0.0 <= self.preemptableCompensation <= 1.0:
            raise RuntimeError("--preemptableCompensation (%f) must be >= 0.0 and <= 1.0",
                               self.preemptableCompensation)

        # Resource requirements
        setOption("defaultMemory", h2b, _check_int(1))
        setOption("defaultCores", float, _check_float(1.0))
        setOption("defaultDisk", h2b, _check_int(1))
        setOption("maxCores", int, _check_int(1))
        setOption("maxMemory", h2b, _check_int(1))
        setOption("maxDisk", h2b, _check_int(1))
        setOption("defaultPreemptable")

        #Misc
        setOption("maxLogFileSize", h2b, _check_int(1))

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return self.__dict__.__hash__()

StorageLocatorHelp = ("The \'storage\' holds persistent information about jobs and files in a "
                      "workflow. The storage must be accessible by all worker nodes."
                      " Depending on the desired implementation, the location should be formatted "
                      "according to one of the following schemes:\n\n"
                      "\tfile:<path>, where <path> is a directory in the file system\n\n"
                      "\taws:<region>:<prefix>, where <region> is the name of an AWS region "
                      "(e.g,, eu-west-1) and <prefix> a string to be prepended to the names of any "
                      "AWS resources in use (e.g., S3 buckets).\n\n "
                      "\tYou may also specify ./foo (equivalent to "
                      "file:./foo or just file:foo) or /bar (equivalent to file:/bar).")

def _addOptions(addGroupFn, config):
    #
    #Core options
    #
    addOptionFn = addGroupFn("Core options",
                             "Specify the location of the workflow and turn on/off statistics.")
    addOptionFn('storage', type=str,
                help="The location of the job store for the workflow. " + StorageLocatorHelp)
    addOptionFn("--workingDir", dest="workingDir", default=None,
                help="Absolute path to directory where temporary files should be placed. "
                "Temp files and folders will be placed in a directory named "
                "*tmp-<workflowID>* within *workingDir* (The workflowID is generated automatically and "
                "will be reported in the workflow logs. Default path is determined by the "
                "the environment variables (TMPDIR, TEMP, TMP) via mkdtemp. These directories "
                "need to exist on all machines running jobs.")
    addOptionFn("--stats", dest="stats", action="store_true", default=None,
                help="Records statistics about the workflow.")
    addOptionFn("--clean", dest="clean", choices=['always', 'onError', 'never', 'onSuccess'],
                default=None,
                help=("Whether to clean the storage upon completion of the program. "
                      "Choices: 'always', 'onError','never', 'onSuccess'. The --stats option requires "
                      "information from the storage upon completion so the storage will never be deleted with"
                      "that flag. If you wish to be able to restart the run, choose \'never\' or \'onSuccess\'. "
                      "Default is \'never\' if stats is enabled, and \'onSuccess\' otherwise"))
    addOptionFn("--cleanWorkDir", dest="cleanWorkDir",
                choices=['always', 'never', 'onSuccess', 'onError'], default='always',
                help=("Whether to delete temporary directories upon completion of a job. Choices: 'always', "
                      "'never', 'onSuccess'. Default = always. "
                      "WARNING: not cleaning temp directories could blow up your disk consumption."))
    addOptionFn("--clusterStats", dest="clusterStats", nargs='?', action='store',
                default=None, const=os.getcwd(),
                help="Write out JSON resource usage statistics to a file. "
                     "The default location for this file is the current working directory, "
                     "but an absolute path can also be passed to specify where this file "
                     "should be written.")
    #
    #Restarting the workflow options
    #
    addOptionFn = addGroupFn("Restart options",
                             "Allow the restart of an existing workflow")
    addOptionFn("--restart", dest="restart", default=None, action="store_true",
                help="TODO: Attempt to restart the existing workflow "
                "at the location pointed to by the --storage option. "
                "Will raise an exception if the workflow does not exist")

    #
    #Batch system options
    #
    addOptionFn = addGroupFn("Batch system options",
                             "Batch system arguments.")
    addOptionFn("--batchSystem", dest="batchSystem", default=None,
                      help=("The type of batch system to run the job(s) with, currently can be one "
                            "of singleMachine or mesos'. default=%s" % config.batchSystem))
    addOptionFn("--scale", dest="scale", default=None,
                help=("A scaling factor to change the value of running cores (or nodes). "
                      "Used in singleMachine batch system. default=%s" % config.scale))
    addOptionFn("--mesosMaster", dest="mesosMasterAddress", default=None,
                help=("The host and port of the Mesos master separated by colon. "
                      "default=%s" % config.mesosMasterAddress))

    #
    #Plasticity options
    #
    addOptionFn = addGroupFn("Plasticity options",
                             "Minimum and maximum number of nodes "
                             "in an hybrid cluster, as well as parameters to control the "
                             "level of provisioning.")

    addOptionFn("--provisioner", dest="provisioner", choices=['aws'],
                help="The provisioner for cluster scaling. The currently supported choices are"
                     "'aws'. The default is %s." % config.provisioner)

    # Taken from Toil:
    for preemptable in (False, True):
        def _addOptionFn(*name, **kwargs):
            name = list(name)
            if preemptable:
                name.insert(-1, 'preemptable' )
            name = ''.join((s[0].upper() + s[1:]) if i else s for i, s in enumerate(name))
            terms = re.compile(r'\{([^{}]+)\}')
            _help = kwargs.pop('help')
            _help = ''.join((term.split('|') * 2)[int(preemptable)] for term in terms.split(_help))
            addOptionFn('--' + name, dest=name,
                        help=_help + ' The default is %s.' % getattr(config, name),
                        **kwargs)

        _addOptionFn('nodeType', metavar='TYPE',
                     help="Node type for {non-|}preemptable nodes. The syntax depends on the "
                          "provisioner used. For AWS this is the name of an EC2 instance "
                          "type, followed by {| a colon and the price in dollar "
                          "to bid for a spot instance}, for example 'c3.8xlarge{|:0.42}'.")

        for p, q in [('min', 'Minimum'), ('max', 'Maximum')]:
            _addOptionFn(p, 'nodes', default=None, metavar='NUM',
                         help=q + " number of {non-|}preemptable nodes in the cluster.")

    # Taken from Toil
#     addOptionFn("--alphaPacking", dest="alphaPacking", default=None,
#                 help=("The total number of nodes estimated to be required to compute the issued "
#                       "jobs is multiplied by the alpha packing parameter to produce the actual "
#                       "number of nodes requested. Values of this coefficient greater than one will "
#                       "tend to over provision and values less than one will under provision. "
#                       "default=%s" % config.alphaPacking))
#     addOptionFn("--betaInertia", dest="betaInertia", default=None,
#                 help=("A smoothing parameter to prevent unnecessary oscillations in the "
#                       "number of provisioned nodes. If the number of nodes is within the beta "
#                       "inertia of the currently provisioned number of nodes then no change is made "
#                       "to the number of requested nodes. default=%s" % config.betaInertia))

    addOptionFn("--scaleInterval", dest="scaleInterval", default=None,
                help=("The interval (seconds) between assessing if the scale of"
                      " the cluster needs to change. default=%s" % config.scaleInterval))

    addOptionFn("--preemptableCompensation", dest="preemptableCompensation",
                default=None,
                help=("The preference to replace preemptable nodes with "
                      "non-preemptable nodes, when preemptable nodes cannot be started for some "
                      "reason. Defaults to %s. This value must be between 0.0 and 1.0, inclusive. "
                      "A value of 0.0 disables such compensation, a value of 0.5 compensates two "
                      "missing preemptable nodes with a non-preemptable one. A value of 1.0 "
                      "replaces every missing preemptable node with a non-preemptable one." %
                      config.preemptableCompensation))
    #
    #Resource requirements
    #
    addOptionFn = addGroupFn("Resource requirements options",
                             "Specify default cores/memory requirements (if not "
                             "specified in jobs descriptions), and to limit the total amount of "
                             "memory/cores requested to the batch system.")
    addOptionFn('--defaultMemory', dest='defaultMemory', default=None, metavar='INT',
                help='Default amount of memory to request for a job. Only applicable to jobs '
                     'that do not specify an explicit value for this requirement. Standard '
                     'suffixes like K, Ki, M, Mi, G or Gi are supported. Default is %s' %
                     print_bytes( config.defaultMemory, symbols='iec' ))
    addOptionFn('--defaultCores', dest='defaultCores', default=None, metavar='FLOAT',
                help='Default number of CPU cores for a job. Only applicable to jobs '
                     'that do not specify an explicit value for this requirement. Fractions of a '
                     'core (for example 0.1) are supported on Mesos '
                     'and singleMachine. Default is %.1f ' % config.defaultCores)
    addOptionFn('--defaultDisk', dest='defaultDisk', default=None, metavar='INT',
                help='Default amount of disk space to dedicate for a job, if not '
                     'specified in the job description. Default is %s' %
                     print_bytes( config.defaultDisk, symbols='iec' ))
    assert not config.defaultPreemptable, 'User would be unable to reset config.defaultPreemptable'
    addOptionFn('--defaultPreemptable', dest='defaultPreemptable', action='store_true')
    addOptionFn('--maxCores', dest='maxCores', default=None, metavar='INT',
                help='The maximum number of CPU cores to dedicate for a job, if '
                     'not specified in the job description. Standard suffixes like '
                     'K, Ki, M, Mi, G or Gi are supported. Default '
                     'is %s' % print_bytes(config.maxCores, symbols='iec'))
    addOptionFn('--maxMemory', dest='maxMemory', default=None, metavar='INT',
                help='The maximum amount of memory to dedicate for a job, if '
                     'not specified in the job description. Standard suffixes like '
                     ' K, Ki, M, Mi, G or Gi are supported. Default '
                     'is %s' % print_bytes( config.maxMemory, symbols='iec'))
    addOptionFn('--maxDisk', dest='maxDisk', default=None, metavar='INT',
                help='The maximum amount of disk space to dedicate for a job, if '
                     'not specified in the job description. Standard suffixes like '
                     'K, Ki, M, Mi, G or Gi are supported. '
                     'Default is %s' % print_bytes(config.maxDisk, symbols='iec'))
    #
    #Misc options
    #
    addOptionFn = addGroupFn("Miscellaneous options", "Miscellaneous options")
    addOptionFn("--maxLogFileSize", dest="maxLogFileSize", default=None,
                help=("The maximum size of a log file (in bytes). Setting "
                      "this option to zero will prevent any truncation. Setting this "
                      "option to a negative value will truncate from the beginning."
                      "Default=%s" % print_bytes(config.maxLogFileSize)))

def addOptions(parser, config=Params()):
    """
    Adds toil options to a parser object, either optparse or argparse.
    """
    # TODO
    #addLoggingOptions(parser) # This adds the logging stuff.
    if isinstance(parser, ArgumentParser):
        def addGroup(headingString, bodyString):
            return parser.add_argument_group(headingString, bodyString).add_argument
        _addOptions(addGroup, config)
    else:
        raise RuntimeError("Unanticipated class passed to addOptions(), %s. Expecting "
                           "argparse.ArgumentParser" % parser.__class__)
