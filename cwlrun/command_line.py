import os
import threading
import logging
import tes

from workflow import WorkFlow
from monitor import MonitorThread

import cwltool.draft2tool
import cwltool.job
from cwltool.errors import WorkflowException
from cwltool.pathmapper import PathMapper
from cwltool.stdfsaccess import StdFsAccess
from cwltool.workflow import defaultMakeTool
from schema_salad.ref_resolver import file_uri

log = logging.getLogger('cloud_provision')

class CommandLineJob(cwltool.job.CommandLineJob):
    def __init__(self):
        super(CommandLineJob, self).__init__()

    # TODO: inspired by 'py-tes', these functions will construct a CWL task,
    # to be executed on desired instances. these functions are not used
    # at the moment, but will be useful one we establish a Mesos cluster
    def create_input_parameter(self, key, value):
        if "contents" in value:
            return tes.Input(
                name=key,
                description="cwl_input:%s" % (key),
                path=value["path"],
                contents=value["contents"],
                type=value["class"].upper()
            )
        else:
            return tes.Input(
                name=key,
                description="cwl_input:%s" % (key),
                url=value["location"],
                path=value["path"],
                type=value["class"].upper()
            )

    def parse_job_order(self, k, v, inputs):
        if isinstance(v, dict):
            if all([i in v for i in ["location", "path", "class"]]):
                inputs.append(self.create_input_parameter(k, v))

                if "secondaryFiles" in v:
                    for f in v["secondaryFiles"]:
                        self.parse_job_order(f["basename"], f, inputs)

            else:
                for sk, sv in v.items():
                    if isinstance(sv, dict):
                        self.parse_job_order(sk, sv, inputs)

                    else:
                        break

        elif isinstance(v, list):
            for i in range(len(v)):
                if isinstance(v[i], dict):
                    self.parse_job_order("%s[%s]" % (k, i), v[i], inputs)

                else:
                    break

        return inputs

    def parse_listing(self, listing, inputs):
        for item in listing:
            if "contents" in item:
                loc = self.fs_access.join(self.tmpdir, item["basename"])
                with self.fs_access.open(loc, "wb") as gen:
                    gen.write(item["contents"])
            else:
                loc = item["location"]

            parameter = tes.Input(
                name=item["basename"],
                description="InitialWorkDirRequirement:cwl_input:%s" % (
                    item["basename"]
                ),
                url=file_uri(loc),
                path=self.fs_access.join(
                    self.docker_workdir, item["basename"]
                ),
                type=item["class"].upper()
                )
            inputs.append(parameter)

        return inputs

    def collect_input_parameters(self):
        inputs = []

        # find all primary and secondary input files
        # joborder declared in cwltool.JobBase
        for k, v in self.joborder.items():
            self.parse_job_order(k, v, inputs)

        # manage InitialWorkDirRequirement
        self.parse_listing(self.generatefiles["listing"], inputs)

        return inputs

    def create_task_msg(self):
        input_parameters = self.collect_input_parameters()
        output_parameters = []

        if self.stdout is not None:
            parameter = tes.Output(
                name="stdout",
                url=self.output2url(self.stdout),
                path=self.output2path(self.stdout)
            )
            output_parameters.append(parameter)

        if self.stderr is not None:
            parameter = tes.Output(
               name="stderr",
               url=self.output2url(self.stderr),
               path=self.output2path(self.stderr)
            )
            output_parameters.append(parameter)

        output_parameters.append(
            tes.Output(
                name="workdir",
                url=self.output2url(""),
                path=self.docker_workdir,
                type="DIRECTORY"
            )
        )

        container = self.find_docker_requirement()

        cpus = None
        ram = None
        disk = None
        preempt = False
        for i in self.requirements:
            if i.get("class", "NA") == "ResourceRequirement":
                cpus = i.get("coresMin", i.get("coresMax", None))
                ram = i.get("ramMin", i.get("ramMax", None))
                ram = ram / 953.674 if ram is not None else None
                disk = i.get("outdirMin", i.get("outdirMax", None))
                disk = disk / 953.674 if disk is not None else None
                preempt = True if i.get("preemptible") else False
            elif i.get("class", "NA") == "DockerRequirement":
                if i.get("dockerOutputDirectory", None) is not None:
                    output_parameters.append(
                        tes.Output(
                            name="dockerOutputDirectory",
                            url=self.output2url(""),
                            path=i.get("dockerOutputDirectory"),
                            type="DIRECTORY"
                        )
                    )

        create_body = tes.Task(
            name=self.name,
            description=self.spec.get("doc", ""),
            executors=[
                tes.Executor(
                    cmd=self.command_line,
                    image_name=container,
                    workdir=self.docker_workdir,
                    stdout=self.output2path(self.stdout),
                    stderr=self.output2path(self.stderr),
                    stdin=self.stdin,
                    environ=self.environment
                )
            ],
            inputs=input_parameters,
            outputs=output_parameters,
            resources=tes.Resources(
                cpu_cores=cpus,
                ram_gb=ram,
                size_gb=disk,
                preemptible=preempt
            ),
            tags={"CWLDocumentId": self.spec.get("id")}
        )

        return create_body

    # TODO: this is not currently called by the application, because it uses
    # cwltool.CommandLineJob executor. We will need this one we create our own
    # executor engine via Mesos
    def run(self):
        this = self

        def launch(this, kwargs):
            super(CommandLineJob, this).run(**kwargs)

        thread = threading.Thread(target=launch, args=(this, self.kwargs))
        thread.start()

    def output2url(self, path):
        if path is not None:
            return file_uri(
                self.fs_access.join(self.outdir, os.path.basename(path))
            )
        return None

    def output2path(self, path):
        if path is not None:
            return self.fs_access.join(self.docker_workdir, path)
        return None

class CommandLineTool(cwltool.draft2tool.CommandLineTool):
    def __init__(self, spec, **kwargs):
        super(CommandLineTool, self).__init__(spec, **kwargs)


class CommandLineWorkflow(WorkFlow):
    def __init__(self, kwargs):
        super(CommandLineWorkflow, self).__init__()

        if kwargs.get("basedir") is not None:
            self.basedir = kwargs.get("basedir")
        else:
            self.basedir = os.getcwd()
        self.fs_access = StdFsAccess(self.basedir)

    def make_exec_tool(self, spec, **kwargs):
        return CommandLineTool(spec, **kwargs)

    def make_tool(self, spec, **kwargs):
        if 'class' in spec and spec['class'] == 'CommandLineTool':
            return self.make_exec_tool(spec, **kwargs)
        else:
            return defaultMakeTool(spec, **kwargs)

