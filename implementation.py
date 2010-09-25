from zope.interface import implements

from twisted.internet import defer

from smac.amqp.models import Address
from smac.api.session import SessionListener
from smac.api.recorder import Recorder
from smac.modules import base

from recorder import IVCRecorder

class IVCRecorderModule(base.ModuleBase):
    """
    A recorder implementation for the IVC-4300 acquisition card and the custom
    developed C extension module.
    """
    
    implements(Recorder.Iface)
    
    def __init__(self, *args, **kwargs):
        super(IVCRecorderModule, self).__init__(*args, **kwargs)
        
        self.sessions = {}
        """A dictionary holding all currently configured acquisition sessions."""
    
    @defer.inlineCallbacks
    def configure_session(self, session):
        print "Configuring module for session {0}".format(session.id)
        self.sessions[session.id] = handler = IVCRecorder(self, session)
        address = Address(routing_key=session.id)
        yield self.amq_server(address, SessionListener, handler, AcquisitionManager.queues)
    
    def acquisition_devices(self):
        return []
