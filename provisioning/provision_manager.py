
from builtins import object
import logging
import datetime
import os
import json
import time

import threading
from threading import Thread

from provisioning.resource_manager import ResourceManager


log = logging.getLogger('cloud_provision')


class ProvisionManager(object):
    """
    A base class for managing computing resources. This object is responsible
    for requirements estimation, costs evaluation and resource aquisition. This
    is obtained by parsing the workflow description and performs an initial
    analysis of the computing requirements for each of the involved steps,
    estimating the amount of computing resources required for the workflow to
    be executed, taking into account the budget specified.

    In our vision, this object corresponds to the Plasticity Manager.

    The constructor takes a Params object, containing "command-line" options
    where parameters needed to configure the resource provisioning are defined.
    This is implemented as a argparser instance.

    Since the resource provisioning affects the underlying cluster, also a
    resource manager object has to be provided.
    """

    def __init__(self, config=None, resManager=None):
        """
        Initialise the resource provisioner.
        The config object should contain all parameters needed to configure
        the provisioner.

        :param config: An argparser-like object, with option to be used
                for configure the infrastructure and instruct the provisioner.
        :param resManager: a resource manager object, able to schedule tasks to
                underlying computing resources, being them a single machine,
                a cluster or a hybrid cloud
        """
        self.config = config
        # config options from command line

        self.resManager = resManager
        # the resource orchestrator (i.e., Mesos)

        self.stats = {}

        self.statsThreads = []
        # pool of statistics threads

        self.stop = False
        # only shutdown the stats threads once

        self.staticNodesDict = {}
        # dictionary where keys are worker nodes (public) IPs, values are
        # information concerning static (i.e., not cloud) resources

    def add_workers(self, nodeType, numNodes, preemp):
        """
        Add worker nodes to the managed resources
        Worker nodes are computing resources (either cloud or on-premise)

        :param nodeType: instance type
        :param numNodes: The number of nodes to add
        :param preemp: whether or not the nodes will be preemptible

        :return: number of nodes successfully added
        """
        raise NotImplementedError


    def terminate_workers(self, nodes):
        """
        Terminate the given nodes

        :param nodes: list of Worker objects
        """
        raise NotImplementedError

    def get_provisioned_workers(self, nodeType, preemptible):
        """
        Get all workers from the provisioner.
        Includes both static and cloud resources

        :param preemptible: Boolean value indicating whether to return
                preemptible nodes or non-preemptible nodes
        :return: list of Worker objects
        """
        raise NotImplementedError

    def get_workers_params(self, nodeType=None):
        """
        The shape of a node managed by this provisioner. The node shape defines
        key properties of a machine, such as its computing power or the time
        between billing intervals.

        :param str nodeType: Worker type name to return the shape of.

        :rtype: Features
        """
        raise NotImplementedError


    # FIXME: the leader of a cluster is the Mesos master.
    #        these functions should not be here
    @classmethod
    def rsync_leader(cls, clusterName, args, **kwargs):
        """
        Rsync to the leader of the cluster with the specified name. The
        arguments are passed directly to Rsync.

        :param clusterName: name of the cluster to target
        :param args: list of string arguments to rsync. Identical to the normal arguments to rsync, but the
           host name of the remote host can be omitted. ex) ['/localfile', ':/remotedest']
        """
        raise NotImplementedError


    @classmethod
    def ssh_leader(cls, clusterName, args, strict=True):
        """
        SSH into the leader instance of the specified cluster with the specified arguments to SSH.

        :param clusterName: name of the cluster to target
        :param args: list of string arguments to ssh.
        :param strict: If False, strict host key checking is disabled. (Enabled by default.)
        """
        raise NotImplementedError


    @classmethod
    def launch_cluster(cls, instanceType, keyName, clusterName, spotBid=None):
        """
        Launch a cluster, where the leader runs on the specified instance type.

        :param instanceType: desired type of the leader instance
        :param keyName: name of the ssh key pair to launch the instance with
        :param clusterName: desired identifier of the cluster
        :param spotBid: how much to bid for the leader instance. If none, use on demand pricing.

        :return:
        """
        raise NotImplementedError
    # FIXME: ----------------------------------------------------


    @classmethod
    def destroy_cluster(cls, clusterName):
        """
        Terminate all (cloud) instances in the specified cluster, and clean up
        all resources associated with the it.

        :param clusterName: identifier of the cluster to terminate.
        """
        raise NotImplementedError


    def shut_down(self, preemptible):
        if not self.stop:
            self.__shutDownStats()
        log.debug('Forcing provisioner to reduce cluster size to zero.')
        totalNodes = self.set_node_count(numNodes=0, preemptible=preemptible, force=True)
        if totalNodes != 0:
            raise RuntimeError('Provisioner was not able to reduce cluster size to zero.')

    def __shutDownStats(self):
        def getFileName():
            extension = '.json'
            file = '%s-stats' % self.config.jobStore
            counter = 0
            while True:
                suffix = str(counter).zfill(3) + extension
                fullName = os.path.join(self.statsPath, file + suffix)
                if not os.path.exists(fullName):
                    return fullName
                counter += 1

        if self.config.clusterStats: # and self.scaleable:
            self.stop = True
            for thread in self.statsThreads:
                thread.join()
            fileName = getFileName()
            with open(fileName, 'w') as f:
                json.dump(self.stats, f)


    def start_stats(self, preemptible):
        thread = Thread(target=self.__gatherStats, args=[preemptible])
        thread.start()
        self.statsThreads.append(thread)

    def check_stats(self):
        for thread in self.statsThreads:
            # propagate any errors raised in the threads execution
            thread.join(timeout=0)

    def __gatherStats(self, preemptible):
        def tupleToDict(nodeInfo):
            # namedtuples don't retain attribute names when dumped to JSON.
            # convert them to dicts instead to improve stats output.
            return dict(memory=nodeInfo.memory,
                        cores=nodeInfo.cores,
                        workers=nodeInfo.workers,
                        time=time.time()
                        )

        stats = {}
        try:
            while not self.stop:
                nodeInfo = self.resManager.getNodes(preemptible)
                for nodeIP in nodeInfo.keys():
                    nodeStats = nodeInfo[nodeIP]
                    if nodeStats is not None:
                        nodeStats = tupleToDict(nodeStats)
                        try:
                            # if the node is already registered update the dictionary with
                            # the newly reported stats
                            stats[nodeIP].append(nodeStats)
                        except KeyError:
                            # create a new entry for the node
                            stats[nodeIP] = [nodeStats]
                time.sleep(60)
        finally:
            threadName = 'Preemptible' if preemptible else 'Non-preemptible'
            log.debug('%s provisioner stats thread shut down successfully.', threadName)
            self.stats[threadName] = stats


    def set_node_count(self, numNodes, preemptible=False, force=False):
        """
        Attempt to grow or shrink the number of workers to the given value, or as
        close a value as possible.
        Return the resulting number of workers in the cluster.

        :param numNodes: Desired size of the cluster
        :param preemptible: whether the added nodes will be preemptible, i.e. whether they
               may be removed spontaneously by the underlying platform at any time.
        :param force: If False, the provisioner is allowed to deviate from the given number
               of nodes. For example, when downsizing a cluster, a provisioner might leave nodes
               running if they have active jobs running on them.

        :return: the number of nodes in the cluster. This value should be close or equal to
                the `numNodes` argument. It represents the closest possible approximation of the
                actual cluster size at the time this method returns.
        """
        workerInstances = self._getWorkersInCluster(preemptible)
        numCurrentNodes = len(workerInstances)
        delta = numNodes - numCurrentNodes
        if delta > 0:
            log.info('Adding %i %s nodes to get to desired cluster size of %i.', delta,
                     'preemptible' if preemptible else 'non-preemptible', numNodes)
            numNodes = numCurrentNodes + self._addNodes(workerInstances,
                                                        numNodes=delta,
                                                        preemptible=preemptible)
        elif delta < 0:
            log.info('Removing %i %s nodes to get to desired cluster size of %i.', -delta,
                     'preemptible' if preemptible else 'non-preemptible', numNodes)
            numNodes = numCurrentNodes - self.__removeNodes(workerInstances,
                                                           numNodes=-delta,
                                                           preemptible=preemptible,
                                                           force=force)
        else:
            log.info('Cluster already at desired size of %i. Nothing to do.', numNodes)
        return numNodes


    # TODO: this has to be completely reviewed in light of boto3 and the rest
    def __removeNodes(self, instances, numNodes, preemptible=False, force=False):
        # If the batch system is scalable, we can use the number of currently running workers on
        # each node as the primary criterion to select which nodes to terminate.
        if isinstance(self.resManager, ResourceManager):
            nodes = self.resManager.get_nodes(preemptible)
            # Join nodes and instances on private IP address.
            nodes = [(instance, nodes.get(instance.private_ip_address)) for instance in instances]
            log.debug('Nodes considered to terminate: %s', ' '.join(map(str, nodes)))

            # Unless forced, exclude nodes with runnning workers. Note that it is possible for
            # the batch system to report stale nodes for which the corresponding instance was
            # terminated already. There can also be instances that the batch system doesn't have
            # nodes for yet. We'll ignore those, too, unless forced.
            nodesToTerminate = []
            for instance, nodeInfo in nodes:
                if force:
                    nodesToTerminate.append((instance, nodeInfo))
                elif nodeInfo is not None and nodeInfo.workers < 1:
                    nodesToTerminate.append((instance, nodeInfo))
                else:
                    log.debug('Not terminating instances %s. Node info: %s', instance, nodeInfo)
            # Sort nodes by number of workers and time left in billing cycle
            nodesToTerminate.sort(key=lambda (instance, nodeInfo): (
                nodeInfo.workers if nodeInfo else 1,
                self._remainingBillingInterval(instance)))
            if not force:
                # don't terminate nodes that still have > 15% left in their allocated (prepaid) time
                nodesToTerminate = [nodeTuple for nodeTuple in nodesToTerminate if self._remainingBillingInterval(nodeTuple[0]) <= 0.15]
            nodesToTerminate = nodesToTerminate[:numNodes]
            if log.isEnabledFor(logging.DEBUG):
                for instance, nodeInfo in nodesToTerminate:
                    log.debug("Instance %s is about to be terminated. Its node info is %r. It "
                              "would be billed again in %s minutes.", instance.id, nodeInfo,
                              60 * self._remainingBillingInterval(instance))
            instances = [instance for instance, nodeInfo in nodesToTerminate]
        else:
            # Without load info all we can do is sort instances by time left in billing cycle.
            instances = sorted(instances, key=self._remainingBillingInterval)
            #instances = [instance for instance in islice(instances, numNodes)]
        log.info('Terminating %i instance(s).', len(instances))
        if instances:
            self._logAndTerminate(instances)
        return len(instances)


class Worker(object):
    '''
    This class represents a managed computing resource: a worker is a
    running instance in some environment, being it cloud or not, and it is
    characterised by a number of run-time parameters. Namely:
        - public-ip;
        - private-ip;
        - instance-name;
        - launch-time;
        - instance-type;
        - preemptability (e.g., spot instances in AWS)

    This class mimics a console behaviour, where partial information
    on a resource is shown.

    FIXME: isn't this redundant with EC2.Instance information?
    this is probably more useful for static (i.e. non-cloud) resources
    '''
    def __init__(self, publicIP, privateIP, name, launchTime, nodeType, preemptible):
        self.publicIP = publicIP
        self.privateIP = privateIP
        self.name = name
        self.launchTime = launchTime
        self.nodeType = nodeType
        self.preemptible = preemptible

    def __str__(self):
        return "%s at %s" % (self.name, self.publicIP)

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(self.publicIP)


    @property
    def publicIP(self):
        return self.publicIP

    @property
    def privateIP(self):
        return self.privateIP

    @property
    def name(self):
        return self.name

    @property
    def launchTime(self):
        return self.launchTime

    @property
    def nodeType(self):
        return self.nodeType

    @property
    def preemptible(self):
        return self.preemptible
