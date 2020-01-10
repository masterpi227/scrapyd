from zope.interface import implementer
from six import iteritems, viewkeys
from random import shuffle
from twisted.internet.defer import DeferredQueue, inlineCallbacks, maybeDeferred, returnValue

from .utils import get_spider_queues
from .interfaces import IPoller

@implementer(IPoller)
class QueuePoller(object):

    def __init__(self, config):
        self.config = config
        self.update_projects()
        self.dq = DeferredQueue()

    @inlineCallbacks
    def poll(self):
        if not self.dq.waiting:
            return
        ps = [k for k in viewkeys(self.queues)]
        shuffle(ps)
        #for p, q in iteritems(self.queues):
        if ps is not None:
            for p in ps:
                q = self.queues[p]
                c = yield maybeDeferred(q.count)
                if c:
                    msg = yield maybeDeferred(q.pop)
                    if msg is not None:  # In case of a concurrently accessed queue
                        returnValue(self.dq.put(self._message(msg, p)))

    def next(self):
        return self.dq.get()

    def update_projects(self):
        self.queues = get_spider_queues(self.config)

    def _message(self, queue_msg, project):
        d = queue_msg.copy()
        d['_project'] = project
        d['_spider'] = d.pop('name')
        return d
