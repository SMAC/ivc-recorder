from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet import defer
from txamqp.protocol import AMQClient
from txamqp.contrib.thrift.client import ThriftTwistedDelegate
from txamqp.queue import TimeoutDeferredQueue, Closed
from txamqp.contrib.thrift.transport import TwistedAMQPTransport
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from smac.python import log

class SMACServerFactory(object):
    """
    @todo
    """

class SMACClientFactory(object):
    
    exchanges = {
        'response': Exchange('amq.direct'),
        'unicast': Exchange('{namespace}.unicast'),
        'broadcast': Exchange('{namespace}.broadcast'),
        'services': Exchange('{namespace}.services', 'topic'),
    }
    
    iprot_factory = TBinaryProtocol.TBinaryProtocolFactory()
    oprot_factory = TBinaryProtocol.TBinaryProtocolFactory()
    
    def __init__(self, client, channel=None):
        self.client = client
        self.client_lock = defer.DeferredLock()
        self.clients = {}
        
        if client.check_0_8():
            self.reply_to = "reply to"
        else:
            self.reply_to = "reply-to"
        
        self.channel = channel or 1
    
    @defer.inlineCallbacks
    def buildClient(self, remote_service, address, distribution=None):
        yield self.client_lock.acquire()
        try:
            routing_key = address.routing_key()
            service_name = remote_service.__name__ + routing_key
            distribution = address.distribution_label() or distribution
            
            key = (remote_service, routing_key, distribution)
            
            try:
                client = self.clients[key]
            except KeyError:
                log.debug("Creating new client for {0} with routing key {1} and distribution {2}".format(
                    remote_service.__name__, routing_key, distribution))
                
                if isinstance(self.channel, int):
                    channel = yield self.client.channel(self.channel)
                    yield channel.channel_open()
                else:
                    # Assume it's already open!
                    channel = self.channel
                
                response_exchange = self.exchanges['response']
                response_queue = Queue(exclusive=True, auto_delete=True)
                
                yield response_queue.declare(channel)
                yield response_queue.bind(response_exchange, response_queue.name)
                consumer_tag = yield response_queue.consume()
                
                service_exchange = self.exchanges[distribution]
                
                service_exchange.format_name(address)
                yield service_exchange.declare(channel)
                
                amqp_transport = TwistedAMQPTransport(channel, service_exchange.name,
                    routing_key, service_name, response_queue.name, self.reply_to)
                
                client = remote_service.Client(amqp_transport, self.oprot_factory)
                client.address = address
                client.factory = self
                
                queue = yield self.client.queue(consumer_tag)
                self.get_next_message(channel, queue, client)
                
                queue = yield self.client.get_return_queue(service_name)
                self.get_next_unroutable_error(channel, queue, client)
                
                self.clients[key] = client
            else:
                log.debug("Using cached client for {0} with routing key {1} and distribution {2}".format(
                    remote_service.__name__, routing_key, distribution))
        finally:
            self.client_lock.release()
        
        defer.returnValue(client)
    
    def parse_message(self, msg, channel, queue, client):
        tag = msg.delivery_tag
        
        transport = TTransport.TMemoryBuffer(msg.content.body)
        iprot = self.iprot_factory.getProtocol(transport)
        
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        
        if rseqid not in client._reqs:
            log.msg('Missing rseqid! fname = %r, rseqid = %s, mtype = %r, routing key = %r, client = %r, msg.content.body = %r' % (fname, rseqid, mtype, msg.routing_key, client, msg.content.body))
            
        method = getattr(client, 'recv_' + fname)
        method(iprot, mtype, rseqid)
        channel.basic_ack(tag, True)
        
        self.get_next_message(channel, queue, client)
    
    def unrouteable_message(self, msg, channel, queue, client):
        transport = TTransport.TMemoryBuffer(msg.content.body)
        iprot = self.iprot_factory.getProtocol(transport)
        
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        
        try:
            d = client._reqs.pop(rseqid)
        except KeyError:
            # KeyError will occur if the remote Thrift method is oneway,
            # since there is no outstanding local request deferred for
            # oneway calls.
            pass
        else:
            type = TTransport.TTransportException.NOT_OPEN,
            msg = 'Unrouteable message, routing key = %r calling function %r' % (msg.routing_key, fname)
            d.errback(TTransport.TTransportException(type, msg))
        
        self.get_next_unroutable_message(channel, queue, client)
    
    def get_next_unroutable_error(self, channel, queue, client):
        d = queue.get()
        d.addCallback(self.unrouteable_message, channel, queue, client)
        d.addErrback(self.catch_closed_queue)
        d.addErrback(self.handle_queue_error)
    
    def get_next_message(self, channel, queue, client):
        d = queue.get()
        d.addCallback(self.parse_message, channel, queue, client)
        d.addErrback(self.catch_closed_queue)
        d.addErrback(self.handle_queue_error)
    
    def catch_closed_queue(self, failure):
        failure.trap(Closed)
        self.handle_closed_queue(failure)
    
    def handle_queue_error(self, failure):
        pass

    def handle_closed_queue(self, failure):
        pass

class ThriftAMQClient(AMQClient, object):
    def __init__(self, *args, **kwargs):
        super(ThriftAMQClient, self).__init__(*args, **kwargs)
        
        self.return_queues_lock = defer.DeferredLock()
        self.return_queues = {}
    
    @defer.inlineCallbacks
    def get_return_queue(self, key):
        yield self.return_queues_lock.acquire()
        try:
            try:
                q = self.return_queues[key]
            except KeyError:
                q = TimeoutDeferredQueue()
                self.return_queues[key] = q
        finally:
            self.return_queues_lock.release()
        defer.returnValue(q)
    
    # Alias for compatibility with ThriftTwistedDelegate
    thriftBasicReturnQueue = get_return_queue

class AMQClientFactory(ReconnectingClientFactory, object):
    """
    Factory for AMQP connections intended to be used by thrift clients.
    Overriding the C{protocol} property with a more general C{AMQClient} class
    should allow a more generic use of the factory.
    """
    
    protocol = ThriftAMQClient
    
    def __init__(self, spec, vhost):
        self.spec = spec
        self.vhost = vhost
        
    def buildProtocol(self, _):
        client = self.protocol(ThriftTwistedDelegate(), self.vhost, self.spec)
        return client
        
    def clientConnectionLost(self, connector, reason):
        log.error('Connection to AMQP broker lost. Reason {0}'.format(reason))
        super(AMQClientFactory, self).clientConnectionLost(connector, reason)
        
    def clientConnectionFailed(self, connector, reason):
        log.error('Connection to AMQP broker failed. Reason {0}'.format(reason))
        super(AMQClientFactory, self).clientConnectionFailed(self, connector, reason)
    

