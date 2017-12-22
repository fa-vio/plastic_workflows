
from __future__ import absolute_import

import os
import shutil
import logging
import time
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from Queue import Queue, Empty

from provisioning import WorkerInfo, InsufficientSystemResources

# from common import cacheDirName
# from storage import shutdownStorage

log = logging.getLogger('cloud_provision')


class ResourceManager(object):
    """
    An interface to a resource orchestrator.

    This object has knowledge of all available processing elements able to
    execute tasks, and is responsible to trigger the execution of tasks on
    elected workers.

    It should accommodate a variable number of worker nodes, which can scale
    up or down depending on overall load.

    In our vision, this object corresponds to a Mesos master, which acts as
    the main resource orchestrator that offers resources to applications
    willing to execute and schedules tasks among available resources.

    In order to create a OO-like interface, the actual manager scheduler
    should subclass this class.
    """

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        """
        Initializes initial state of the object

        :param config: An argparser-like object, with option to be used
                for configure the infrastructure and instruct the provisioner.
        :param maxCores: the maximum number of cores for a job
        :param maxMemory: the maximum amount of memory for a job, in bytes
        :param maxDisk: the maximum amount of disk space for a job, in bytes
        """
        super(ResourceManager, self).__init__()
        self.config = config
        self.maxCores = maxCores
        self.maxMemory = maxMemory
        self.maxDisk = maxDisk

        self.environment = {}
        self.workerInfo = WorkerInfo(workDir=self.config.workDir,
                                                   workflowID=self.config.workflowID,
                                                   cleanWorkDir=self.config.cleanWorkDir)

    def check_resource_request(self, memory, cores, disk):
        """
        Check if resource request is greater than available or allowed.

        :param memory: amount of memory being requested, in bytes
        :param  cores: number of cores being requested
        :param   disk: amount of disk space being requested, in bytes

        :raise InsufficientSystemResources
        """
        assert memory is not None
        assert disk is not None
        assert cores is not None
        if cores > self.maxCores:
            raise InsufficientSystemResources('cores', cores, self.maxCores)
        if memory > self.maxMemory:
            raise InsufficientSystemResources('memory', memory, self.maxMemory)
        if disk > self.maxDisk:
            raise InsufficientSystemResources('disk', disk, self.maxDisk)


    @abstractmethod
    def get_nodes(self, preemptible=None):
        """
        Get node identifiers of preemptable or non-preemptable workers.

        :param preemptible: If True preemptable nodes will be returned.
                If None, all nodes will be returned.

        :rtype: dict[str,NodeInfo]
        """
        raise NotImplementedError()


    @abstractmethod
    def supports_hot_deployment(self):
        """
        Whether this batch system supports hot deployment of the user script itself. If it does,
        the :meth:`set_user_script` can be invoked to set the resource object representing the user
        script.

        Note to implementors: If your implementation returns True here, it should also override

        :rtype: bool
        """
        raise NotImplementedError()


    @abstractmethod
    def supports_worker_cleanup(self):
        """
        Indicates whether this batch system invokes :meth:`workerCleanup` after the last job for
        a particular workflow invocation finishes. Note that the term *worker* refers to an
        entire node, not just a worker process. A worker process may run more than one job
        sequentially, and more than one concurrent worker process may exist on a worker node,
        for the same workflow. The batch system is said to *shut down* after the last worker
        process terminates.

        :rtype: bool
        """
        raise NotImplementedError()


    def set_user_script(self, userScript):
        """
        Set the user script for this workflow. This method must be called before the first job is
        issued to this batch system, and only if :meth:`supports_hot_deployment` returns True,
        otherwise it will raise an exception.

        :param toil.resource.Resource userScript: the resource object representing the user script
               or module and the modules it depends on.
        """
        raise NotImplementedError()

    @abstractmethod
    def issue_batch_job(self, jobNode):
        """
        Issues a job with the specified command to the batch system and returns a unique jobID.

        :param str command: the string to run as a command,

        :param int memory: int giving the number of bytes of memory the job needs to run

        :param float cores: the number of cores needed for the job

        :param int disk: int giving the number of bytes of disk space the job needs to run

        :param booleam preemptable: True if the job can be run on a preemptable node

        :return: a unique jobID that can be used to reference the newly issued job
        :rtype: int
        """
        raise NotImplementedError()

    @abstractmethod
    def kill_batch_jobs(self, jobIDs):
        """
        Kills the given job IDs.

        :param list[int] jobIDs: list of IDs of jobs to kill
        """
        raise NotImplementedError()

    # FIXME: Return value should be a set (then also fix the tests)

    @abstractmethod
    def get_issued_batch_job_ids(self):
        """
        Gets all currently issued jobs

        :return: A list of jobs (as jobIDs) currently issued (may be running, or may be
                 waiting to be run). Despite the result being a list, the ordering should not
                 be depended upon.
        :rtype: list[str]
        """
        raise NotImplementedError()

    @abstractmethod
    def get_running_batch_job_ids(self):
        """
        Gets a map of jobs as jobIDs that are currently running (not just waiting)
        and how long they have been running, in seconds.

        :return: dictionary with currently running jobID keys and how many seconds they have
                 been running as the value
        :rtype: dict[str,float]
        """
        raise NotImplementedError()

    @abstractmethod
    def get_updated_batch_job(self, maxWait):
        """
        Returns a job that has updated its status.

        :param float maxWait: the number of seconds to block, waiting for a result

        :rtype: (str, int)|None
        :return: If a result is available, returns a tuple (jobID, exitValue, wallTime).
                 Otherwise it returns None. wallTime is the number of seconds (a float) in
                 wall-clock time the job ran for or None if this batch system does not support
                 tracking wall time. Returns None for jobs that were killed.
        """
        raise NotImplementedError()

    @abstractmethod
    def shutdown(self):
        """
        Called at the completion of a toil invocation.
        Should cleanly terminate all worker threads.
        """
        raise NotImplementedError()


    def set_env(self, name, value=None):
        """
        Set an environment variable for the worker process before it is
        launched.

        This method overrides specific environment variables before the worker
        is launched.

        :param name: the environment variable to be set on the worker.
        :param value: if given, assigns this value to the varable identified by
                'name'. If None, variable's current value will be used.

        :raise RuntimeError: if value is None and the name cannot be found in
                the environment
        """
        if value is None:
            try:
                value = os.environ[name]
            except KeyError:
                raise RuntimeError("%s does not exist in current environment", name)
        self.environment[name] = value


    def __getResultsFileName(self, wfPath):
        """
        Get a path where to store results. Some batch systems currently use
        this, and only work if locator is file.

        TODO: finish this once we have tools for proper handling of workflows
        and storage
        """
        # Use  parser to extract the path and type
#         locator, filePath = Toil.parseLocator(wfPath)
#         assert locator == "file"
#         return os.path.join(filePath, "results.txt")

    @classmethod
    def get_rescue_batch_job_frequency(cls):
        """
        Get the period of time to wait (floating point, in seconds) between checking for
        missing/overlong jobs.
        """
        raise NotImplementedError()

    @staticmethod
    def workerCleanup(info):
        """
        Clean up the worker node before shutdown.

        TODO: finish this when we have tools for proper handling of workflows
        and storage

        :param info: A WorkerInfo named tuple, consisting of all the relevant
                information for cleaning up the worker.
        """
        assert isinstance(info, WorkerInfo)
        # TODO
#         workflowDir = Toil.getWorkflowDir(info.workflowID, info.workDir)
#         workflowDirContents = os.listdir(workflowDir)
#         shutdownFileStore(workflowDir, info.workflowID)
#         if (info.cleanWorkDir == 'always'
#             or info.cleanWorkDir in ('onSuccess', 'onError')
#             and workflowDirContents in ([], [cacheDirName(info.workflowID)])):
#             shutil.rmtree(workflowDir)




class NodeInfo(namedtuple("_NodeInfo", "cores memory workers")):
    """
    The cores attribute  is a floating point value between 0 (all cores idle) and 1 (all cores
    busy), reflecting the CPU load of the node.

    The memory attribute is a floating point value between 0 (no memory used) and 1 (all memory
    used), reflecting the memory pressure on the node.

    The workers attribute is an integer reflecting the number of currently active workers
    on the node.
    """


class AbstractScalableBatchSystem(ResourceManager):
    """
    A batch system that supports a variable number of worker nodes.
    Used by :class:`toil.provisioners.clusterScaler.ClusterScaler` to scale the number of worker nodes
    in the cluster
    """


