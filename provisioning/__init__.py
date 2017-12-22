from collections import namedtuple

Features = namedtuple("Feat", "memory cores disk preemptible")
"""
The features required by a Job: every command line job described in CWL should
have a list of computing requirements, in terms of RAM memory, number of cores,
disk size and whether it can be run on preemptibile instances.
They are stored in this structure, and can be used to tune the resource
provisioning.
"""


WorkerInfo = namedtuple("WorkerInfo", ("workDir", "workflowID", "cleanWorkDir"))
'''
An object containing the information required for worker cleanup:
    - A path to the value of config.workDir (where the cache would go)
    - The value of config.workflowID (used to identify files specific to this workflow)
    - The value of the cleanWorkDir flag
'''


class InsufficientSystemResources(Exception):
    """
    An exception raised when a job needs resources than currently avaliable
    """
    def __init__(self, resource, requested, available):
        """
        Creates an instance of this exception that indicates which resource is insufficient for current
        demands, as well as the amount requested and amount actually available.

        :param str resource: string representing the resource type

        :param int|float requested: the amount of the particular resource requested that resulted
               in this exception

        :param int|float available: amount of the particular resource actually available
        """
        self.requested = requested
        self.available = available
        self.resource = resource

    def __str__(self):
        return 'Requesting more {} than either physically available, or enforced by --max{}. ' \
               'Requested: {}, Available: {}'.format(self.resource, self.resource.capitalize(),
                                                     self.requested, self.available)
