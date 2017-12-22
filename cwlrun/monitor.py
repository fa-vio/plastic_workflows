import threading
import logging

log = logging.getLogger('cloud_provision')

class MonitorThread(threading.Thread):
    '''
    This class is intended to be used as a monitoring thread: it is responsible
    for polling the task executor and retrieve information about execution
    state and exit status.
    '''
    def __init__(self, operation, poll_interval=1, poll_retries=10):
        super(MonitorThread, self).__init__()
        self.daemon = True
        self.operation = operation
        self.id = operation.id
        self.poll_interval = poll_interval
        self.poll_retries = poll_retries
        self.success = None

    def poll(self):
        raise Exception('MonitorThread.poll() not implemented')

    def is_done(self, operation):
        raise Exception('MonitorThread.is_done(operation) not implemented')

    def complete(self, operation):
        raise Exception('MonitorThread.complete(operation) not implemented')

    def run(self):
        raise Exception('MonitorThread.run() not implemented')
