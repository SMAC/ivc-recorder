

from twisted.internet import defer, reactor, protocol

from smac.acquisition import AcquisitionSetup
from smac.session.remote import Handler
from smac.tasks import Task

from sender import OutgoingFileTransfer

import ivc
import os
from twisted.python import log


class RecordingTask(Task):
    def __init__(self, parent, session):
        super(RecordingTask, self).__init__(sessid=session.id, parent=parent)
        
        self.session = session
    
    @defer.inlineCallbacks
    def run(self):
        started = defer.Deferred()
        self.serverStopped = defer.Deferred()
        self.processStopped = defer.Deferred()
        
        fact = protocol.Factory()
        fact.protocol = ivc.IVC4300Protocol
        fact.onConnectionMade = started
        fact.onConnectionLost = self.serverStopped
        
        proc = ivc.IVC4300Process(self.processStopped)
        
        executable = "C:/smacCapture/capture2.exe"
        path, bin = os.path.split(executable)
        
        PORT = 6544
        port = reactor.listenTCP(PORT, fact)
        reactor.spawnProcess(proc, executable, [bin,], {}, path)
        
        self.protocol = yield started
        self.portStopped = defer.maybeDeferred(port.stopListening)
        self.portStopped.addCallback(lambda _: log.msg("Stopped listening"))
        yield self.protocol.start()
        
        print "Start recording session {0} (parent task is {1})".format(self.session.id, self.parent)
    
    @defer.inlineCallbacks
    def stop(self):
        self.protocol.stop().addCallback(lambda _: log.msg("Stop signal sent"))
        
        d1 = self.processStopped.addCallback(lambda r: log.msg("Process exited") or r)
        d2 = self.serverStopped.addCallback(lambda _: log.msg("Server stopped"))
        
        res = yield defer.gatherResults([self.portStopped, d1, d2])
        
        super(RecordingTask, self).complete(res[1])


class IVCRecorder(Handler):
    
    def __init__(self, *args, **kwargs):
        super(IVCRecorder, self).__init__(*args, **kwargs)
        
        self.setup = AcquisitionSetup(self.session.setup)
    
    @defer.inlineCallbacks
    def record(self, task):
        print "Received recording start signal", task
        
        task = RecordingTask(task, self.session)
        yield self.host.task_register.add(task)
        task.start()
    
    #@defer.inlineCallbacks
    #def archive(self, task):
    #    for r, d, s in self.setup.streams(self.host.address):
    #        path = join(self.host.basedir, self.host.streams[d][0], self.host.streams[d][1][s])
    #        destinations = self.setup.archivers((r, d, s))
    #        
    #        transfer = OutgoingFileTransfer(path, path)
    #        transfer.parent = task
    #        
    #        yield self.host.task_register.add(transfer)
    #        
    #        def finish(result):
    #            print "Finished"
    #            import pprint
    #            pprint.pprint(result)
    #        
    #        transfer.start(self.host.amq_service.parent.parent, destinations)
    #        transfer.finish().addCallback(finish)
    #    