
import hashlib
import os
import uuid

from twisted.internet import defer, task

from smac import tasks
from smac.amqp import models
from smac.amqp.service import AMQService
from smac.api.archiver import FileReceiver
from smac.conf import settings
from smac.python import log


def sizeof_fmt(num):
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


class FileTransferCompleted(Exception):
    pass


class FileTransferService(AMQService):
    def __init__(self, deferred, *args, **kwargs):
        super(FileTransferService, self).__init__(*args, **kwargs)
        self._deferred = deferred
    
    @defer.inlineCallbacks
    def startService(self):
        yield super(FileTransferService, self).startService()
        print "Starting file transfer"
        self._deferred.callback(self)
    


class OutgoingFileTransfer(tasks.Task):
    chunk_size = 128 * 1024
    
    def __init__(self, filename, source):
        if isinstance(source, basestring):
            source = open(source, 'r')
        
        self.key = str(uuid.uuid1())
        self.filename = filename
        self.source = source
        self.size = os.path.getsize(source.name)
        self.checksum = hashlib.sha512()
        self.sent = 0
        self.destinations = {}
        
        super(OutgoingFileTransfer, self).__init__(self.key)
    
    @property
    def status_text(self):
        if self._status_text:
            return self._status_text
        
        l = len(self.destinations)
        
        if not l:
            dests = 'message broker'
        elif l is 1:
            dests = str(self.destinations.keys()[0])
        else:
            dests = '{0} destinations'.format(l)
        
        f = os.path.join(*self.filename.rsplit('/', 2)[1:])
        return u'Sending {0} to {1} ({2} remaining)'.format(f,
                dests, sizeof_fmt(max(0, self.size - self.sent)))
    
    @defer.inlineCallbacks
    def run(self, application, destinations):
        yield self.setup(application, destinations)
        
        if not self.destinations:
            log.warn("No destinations selected, aborting transfer")
            return
        
        self.transfer()
    
    @defer.inlineCallbacks
    def setup(self, application, destinations):
        yield self._setup_service(application)
        yield self._setup_destinations(destinations)
        self.client = yield self._setup_client()
    
    def _setup_destinations(self, dests):
        deferreds = []
        
        if models.IAddress.providedBy(dests):
            dests = (dests,)
        else:
            dests = set(dests)
        self.destinations = dict(zip(dests, [None for i in dests]))
        
        for dest in self.destinations.keys():
            d = self.service.client_factory.build_client(dest)
            d.addCallback(self._start_upload, dest)
            d.addErrback(self._discard_destination, dest)
            deferreds.append(d)
        
        return defer.DeferredList(deferreds)
    
    def _setup_service(self, application):
        started = defer.Deferred()
        self.service = FileTransferService(
            started, settings.amqp.spec,
            settings.amqp.host, settings.amqp.port, settings.amqp.vhost,
            settings.amqp.user, settings.amqp.password, settings.amqp.channel
        )
        self.service.setName("AMQ Connection layer for {0} transfer".format(self.filename))
        self.service.setServiceParent(application)
        return started
    
    def _setup_client(self):
        address = models.Address(routing_key=self.key)
        return self.service.client_factory.build_client(address, FileReceiver,
                distribution='transfers', cache=False)
    
    def _start_upload(self, client, dest):
        self.destinations[dest] = client
        return client.start_upload(self.key, self.filename, self.size, self.parent)
    
    def _discard_destination(self, failure, dest):
        del self.destinations[dest]
        log.error("Destination %s discarded, error follows" % dest)
        log.error(failure)
        
    def _send_data(self, source, chunk_size):
        data = source.read(chunk_size)
        
        if data:
            self.checksum.update(data)
            self.sent += chunk_size
            
            self.completed = self.sent * 1.0 / self.size
            return self.client.send_data_chunk(data)
        else:
            return defer.fail(FileTransferCompleted())
    
    @defer.inlineCallbacks
    def _finalize(self, failure):
        failure.trap(FileTransferCompleted)
        dests, c, k = self.destinations, self.checksum.hexdigest(), self.key
        result = yield defer.DeferredList([d.finalize_upload(k, c) for d in dests.values()])
        self.complete("Transfer of {0} completed in {1} seconds".format(self.filename, self.elapsed))
        yield self.service.stopService()
        defer.returnValue(result)
    
    def transfer(self):
        work = task.LoopingCall(self._send_data, self.source, self.chunk_size)
        d = work.start(0)
        d.addErrback(self._finalize).addErrback(self.finished.errback)
        
        return d
    


