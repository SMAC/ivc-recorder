import os

from twisted.internet import reactor, protocol, error, defer
from twisted.python import log, failure
from twisted.protocols import basic


class UnexpectedData(Exception):
    pass


class IVC4300Protocol(basic.LineReceiver, object):
    delimiter = '\n'
    
    def __init__(self):
        self.data = defer.Deferred()
    
    def connectionMade(self):
        log.msg("Process connected")
        self.factory.onConnectionMade.callback(self)
    
    def connectionLost(self, failure):
        failure.trap(error.ConnectionDone)
        self.factory.onConnectionLost.callback(self)
    
    @defer.inlineCallbacks
    def start(self):
        yield self.waitForCommand('Init OK')
        yield self.sendStart()
    
    @defer.inlineCallbacks
    def stop(self):
        yield self.sendStop()
    
    def lineReceived(self, data):
        self.data, d = defer.Deferred(), self.data
        d.callback(data)
    
    def sendStart(self):
        log.msg("Sending start")
        self.sendLine('START')
        return self.waitForCommand('OK')
    
    def sendStop(self):
        log.msg("Sending stop")
        self.sendLine('STOP')
        return self.waitForCommand('OK')
    
    def waitForCommand(self, cmd):
        log.msg("Waiting for '{0}'".format(cmd))
        d = defer.Deferred()
        
        def fire(data, d, cmd):
            if data == cmd:
                log.msg("Command '{0}' received".format(cmd))
                d.callback(None)
            else:
                log.msg("Unexpected '{0}' received".format(data))
                d.errback(failure.Failure(UnexpectedData(data)))
            return None
        
        self.data.addCallback(fire, d, cmd)
        
        return d
    


class IVC4300Process(protocol.ProcessProtocol):
    
    def __init__(self, stopped):
        self.processStopped = stopped
        self.out = ''
    
    def connectionMade(self):
        log.msg("Process started")
    
    def outReceived(self, data):
        self.out += data
    
    def processEnded(self, failure):
        failure.trap(error.ProcessDone)
        self.processStopped.callback(self.out)
    


class IVC4300Recorder(object):
    
    @defer.inlineCallbacks
    def start(self):
        started = defer.Deferred()
        self.serverStopped = defer.Deferred()
        self.processStopped = defer.Deferred()
        
        fact = protocol.Factory()
        fact.protocol = IVC4300Protocol
        fact.onConnectionMade = started
        fact.onConnectionLost = self.serverStopped
        
        proc = IVC4300Process(self.processStopped)
        
        executable = "C:/smacCapture/capture2.exe"
        path, bin = os.path.split(executable)
        
        PORT = 6544
        port = reactor.listenTCP(PORT, fact)
        reactor.spawnProcess(proc, executable, [bin,], {}, path)
        
        self.protocol = yield started
        self.portStopped = defer.maybeDeferred(port.stopListening)
        self.portStopped.addCallback(lambda _: log.msg("Stopped listening"))
        yield self.protocol.start()
    
    @defer.inlineCallbacks
    def stop(self):
        self.protocol.stop().addCallback(lambda _: log.msg("Stop signal sent"))
        
        d1 = self.processStopped.addCallback(lambda r: log.msg("Process exited") or r)
        d2 = self.serverStopped.addCallback(lambda _: log.msg("Server stopped"))
        
        res = yield defer.gatherResults([self.portStopped, d1, d2])
        
        defer.returnValue(res[1])
    
