import sys
import logging
import signal
import pkg_resources
import yaml

import cwltool.main

from __init__ import __version__
from local_wf import LocalWorkflow
from command_line import CommandLineWorkflow

log = logging.getLogger('cloud_provision')
log.setLevel(logging.INFO)
console = logging.StreamHandler()
log.addHandler(console)

def versionstring():
    pkg = pkg_resources.require("cwltool")
    if pkg:
        cwltool_ver = pkg[0].version
    else:
        cwltool_ver = "unknown"
    return "%s %s with cwltool %s" % (sys.argv[0], __version__, cwltool_ver)

def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = cwltool.main.arg_parser()
    parser = add_args(parser)
    newargs = parser.parse_args(args)

    if not len(args) >= 1:
        print(versionstring())
        print("CWL document required, no input file provided")
        parser.print_usage()
        return 1

    if newargs.version:
        print(versionstring())
        return 0

    if newargs.quiet:
        log.setLevel(logging.WARN)

    if newargs.debug:
        log.setLevel(logging.DEBUG)

    # TODO: remove
    if newargs.local is not None:
        with open(newargs.local) as handle:
            config = yaml.load(handle.read())
            workflow = LocalWorkflow(config, newargs)
    # TODO: remove
    elif newargs.local_configs is not None and len(newargs.local_configs):
        d = {}
        for k, v in newargs.local_configs:
            d[k] = v
        workflow = LocalWorkflow(d, newargs)
    else:
        workflow = CommandLineWorkflow({})

            # setup signal handler
    def signal_handler(*args):
        log.info(
            "recieved ctrl+c signal"
        )
        log.info(
            "terminating thread(s)..."
        )
        log.warning(
            "Running processes %s may keep running" %
            ([t.id for t in workflow.threads])
        )
        sys.exit(1)


    signal.signal(signal.SIGINT, signal_handler)


    return cwltool.main.main(
        args=newargs,
        executor=workflow.executor,
        makeTool=workflow.make_tool,
        versionfunc=versionstring,
        logger_handler=console
    )

def add_args(parser):
    parser.add_argument("--local", default=None, help="Task Execution on Local System")
    parser.add_argument("-t", dest="local_configs", default=None, action="append", nargs=2)
    return parser


# if __name__ == '__main__':
#     sys.exit(main(sys.argv[1:]))
