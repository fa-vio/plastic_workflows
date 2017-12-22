import os
import json
import shutil
import logging
import hashlib
from pprint import pformat

import cwltool.draft2tool
from cwltool.pathmapper import MapperEnt

from monitor import MonitorThread
from workflow import WorkFlow, WorkflowJob
from io_utilities import IOutilities

try:
    import requests
except ImportError:
    pass

log = logging.getLogger('cloud_provision')
BASE_MOUNT = "/mnt"

class LocalStoragePathMapper(cwltool.pathmapper.PathMapper):
    """
    This class extends the 'PathMapper' object in cwltool.
    """

    def __init__(self, referenced_files, basedir, store_base, **kwargs):
        self.store_base = store_base
        self.setup(referenced_files, basedir)

    def setup(self, referenced_files, basedir):
        log.debug("PATHMAPPER: " + pformat(referenced_files))
        self._pathmap = {}
        for src in referenced_files:
            log.debug('SOURCE: ' + str(src))
            if src['location'].startswith("fs://"):
                target_name = os.path.basename(src['location'])
                self._pathmap[src['location']] = MapperEnt(
                    resolved=src['location'],
                    target=os.path.join(BASE_MOUNT, target_name),
                    type=src['class']
                )
            elif src['location'].startswith("file://"):
                src_path = src['location'][7:]
                log.debug("Copying %s to shared %s" % (src['location'], self.store_base))
                dst = os.path.join(self.store_base, os.path.basename(src_path))
                shutil.copy(src_path, dst)
                location = "fs://%s" % (os.path.basename(src['location']))
                self._pathmap[src['location']] = MapperEnt(
                    resolved=location,
                    target=os.path.join(BASE_MOUNT, os.path.basename(src['location'])),
                    type=src['class']
                )
            else:
                raise Exception("Unknown file source: %s" %(src['location']))
        log.debug('PATHMAP: ' + pformat(self._pathmap))

class LocalService:
    def __init__(self, addr):
        self.addr = addr

    def submit(self, task):
        r = requests.post("%s/v1/jobs" % (self.addr), json=task)
        data = r.json()
        if 'Error' in data:
            raise Exception("Request Error: %s" % (data['Error']) )
        return data['value']

    def get_job(self, job_id):
        r = requests.get("%s/v1/jobs/%s" % (self.addr, job_id))
        return r.json()

    def get_server_metadata(self):
        r = requests.get("%s/v1/jobs-service" % (self.addr))
        return r.json()

class LocalWorkflow(WorkFlow):
    def __init__(self, config, args):
        super(LocalWorkflow, self).__init__(config)
        self.args = args
        self.service = LocalService(config['url'])
        meta = self.service.get_server_metadata()
        if meta['storageConfig'].get("storageType", "") == "sharedFile":
            self.local_path = IOutilities(meta['storageConfig']['baseDir'], "output")
        self.output_dir = os.path.join(self.local_path.protocol(), "outdir")

    def create_parameters(self, puts, pathmapper, create=False):
        parameters = []
        for put, path in puts.items():
            if not create:
                ent = pathmapper.mapper(path)
                if ent is not None:
                    parameter = {
                        'name': put,
                        'description': "cwl_input:%s" % (put),
                        'location' : ent.resolved,
                        'path': ent.target
                    }
                    parameters.append(parameter)
            else:
                parameter = {
                    'name' : put,
                    'description' : "cwl_output:%s" %(put),
                    'location' : os.path.join(self.local_path.protocol(), path),
                    'path' : os.path.join(BASE_MOUNT, path)
                }

        return parameters

    def create_task(self, container, command, inputs, outputs, volumes, config, pathmapper, stdout=None, stderr=None):
        input_parameters = self.create_parameters(inputs, pathmapper)
        output_parameters = self.create_parameters(outputs, pathmapper, create=True)
        workdir = os.path.join(BASE_MOUNT, "work")

        log.debug("LOCAL_URI: " + self.local_path.protocol())

        output_parameters.append({
            'name': 'workdir',
            'location' : os.path.join(self.local_path.protocol(), "work"),
            'path' : workdir,
            'class' : 'Directory',
            'create' : True
        })

        create_body = {
            'projectId': "test",
            'name': 'funnel workflow',
            'description': 'CWL TES task',
            'docker' : [{
                'cmd': command,
                'imageName': container,
                'workdir' : workdir
            }],
            'inputs' : input_parameters,
            'outputs' : output_parameters,
            'resources' : {
                'volumes': [{
                    'name': 'data',
                    'mountPoint': BASE_MOUNT,
                    'sizeGb': 10,
                }],
                'minimumCpuCores': 1,
                'minimumRamGb': 1,
            }
        }

        if stdout is not None:
            create_body['docker'][0]['stdout'] = stdout[0]
            parameter = {
                'name': 'stdout',
                'description': 'tool stdout',
                'location' : stdout[1],
                'path': stdout[0]
            }
            create_body['outputs'].append(parameter)

        if stderr is not None:
            create_body['docker'][0]['stderr'] = stderr[0]
            parameter = {
                'name': 'stderr',
                'description': 'tool stderr',
                'location' : stderr[1],
                'path': stderr[0]
            }
            create_body['outputs'].append(parameter)

        return create_body

    def make_exec_tool(self, spec, **kwargs):
        return LocalWorkflowTool(spec, self, fs_access=self.local_path, **kwargs)

class LocalWorkflowTool(cwltool.draft2tool.CommandLineTool):
    def __init__(self, spec, wf, local_path, **kwargs):
        super(LocalWorkflowTool, self).__init__(spec, **kwargs)
        self.spec = spec
        self.workflow = wf
        self.local_path = local_path

    def makeJobRunner(self):
        return LocalWorkflowJob(self.spec, self.workflow, self.local_path)

    def makePathMapper(self, reffiles, stagedir, **kwargs):
        m = self.workflow.service.get_server_metadata()
        if m['storageConfig'].get('storageType', "") == "sharedFile":
            return LocalStoragePathMapper(reffiles, store_base=m['storageConfig']['baseDir'], **kwargs)

class LocalWorkflowJob(WorkflowJob):
    def __init__(self, spec, workflow, local_path):
        super(LocalWorkflowJob,self).__init__(spec, workflow)
        self.running = False
        self.local_path = local_path

    def run(self, dry_run=False, pull_image=True, **kwargs):
        id = self.spec['id']

        log.debug('SPEC: ' + pformat(self.spec))
        log.debug('JOBORDER: ' + pformat(self.joborder))
        log.debug('GENERATEFILES: ' + pformat(self.generatefiles))

        #prepare the inputs
        inputs = {}
        for k, v in self.joborder.items():
            if isinstance(v, dict):
                inputs[k] = v['location']

        for listing in self.generatefiles['listing']:
            if listing['class'] == 'File':
                with self.local_path.open(listing['basename'], 'wb') as gen:
                    gen.write(listing['contents'])

        output_path = self.workflow.config.get('outloc', "output")

        log.debug('SPEC_OUTPUTS: ' + pformat(self.spec['outputs']))
        outputs = {output['id'].replace(id + '#', '') :
                   output['outputBinding']['glob'] for output in self.spec['outputs'] if 'outputBinding' in output}
        log.debug('PRE_OUTPUTS: ' + pformat(outputs))

        stdout_path=self.spec.get('stdout', None)
        stderr_path=self.spec.get('stderr', None)

        if stdout_path is not None:
            stdout = (self.output2path(stdout_path), self.output2location(stdout_path))
        else:
            stdout = None
        if stderr_path is not None:
            stderr = (self.output2path(stderr_path), self.output2location(stderr_path))
        else:
            stderr = None

        container = self.find_docker_requirement()

        task = self.workflow.create_task(
            container=container,
            command=self.command_line,
            inputs=inputs,
            outputs=outputs,
            volumes=BASE_MOUNT,
            config=self.workflow.config,
            pathmapper=self.pathmapper,
            stderr=stderr,
            stdout=stdout
        )

        log.debug("TASK: " + pformat(task))

        task_id = self.workflow.service.submit(task)
        operation = self.workflow.service.get_job(task_id)
        collected = {output: {'location': "fs://output/" + outputs[output], 'class': 'File'} for output in outputs}

        log.debug("OPERATION: " + pformat(operation))
        log.debug('COLLECTED: ' + pformat(collected))

        monitor = LocalWorkflowMonitor(
            service=self.workflow.service,
            operation=operation,
            outputs=collected,
            callback=self.jobCleanup
        )

        self.workflow.add_thread(monitor)
        monitor.start()

    def jobCleanup(self, operation, outputs):
        log.debug('OPERATION: ' + pformat(operation))
        log.debug('OUTPUTS: ' + pformat(outputs))
        # log.debug('CWL_OUTPUT_PATH: ' + pformat(self.local_path._abs("cwl.output.json")))

        final = {}
        if self.local_path.exists("work/cwl.output.json"):
            log.debug("Found cwl.output.json file")
            with self.local_path.open("work/cwl.output.json", 'r') as args:
                cwl_output = json.loads(args.read())
            final.update(cwl_output)
        else:
            for output in self.spec['outputs']:
                type = output['type']
                if isinstance(type, dict):
                    if 'type' in type:
                        if type['type'] == 'array':
                            with self.local_path.open("work/cwl.output.json", 'r') as args:
                                final = json.loads(args.read())
                elif type == 'File':
                    id = output['id'].replace(self.spec['id'] + '#', '')
                    binding = output['outputBinding']['glob']
                    glob = self.local_path.glob(binding)
                    log.debug('GLOB: ' + pformat(glob))
                    with self.local_path.open(glob[0], 'rb') as handle:
                        contents = handle.read()
                        size = len(contents)
                        checksum = hashlib.sha1(contents)
                        hex = "sha1$%s" % checksum.hexdigest()

                    collect = {
                        'location': os.path.basename(glob[0]),
                        'class': 'File',
                        'size': size,
                        'checksum': hex
                    }

                    final[id] = collect

        self.output_callback(final, 'success')

    def output2location(self, path):
        return "fs://output/" + os.path.basename(path)

    def output2path(self, path):
        return "/mnt/" + path


class LocalWorkflowMonitor(MonitorThread):
    def __init__(self, service, operation, outputs, callback):
        super(LocalWorkflowMonitor, self).__init__(operation)
        self.service = service
        self.outputs = outputs
        self.callback = callback

    def poll(self):
        return self.service.get_job(self.operation['jobId'])

    def is_done(self, operation):
        return operation['state'] in ['Complete', 'Error']

    def complete(self, operation):
        self.callback(operation, self.outputs)

