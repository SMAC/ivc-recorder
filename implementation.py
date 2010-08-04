from zope.interface import implements

from twisted.internet import defer
from os.path import join, isdir, realpath
from os import listdir

from smac.amqp.models import Address
from smac.api.session import SessionListener
from smac.api.recorder import Recorder
from smac.api.recorder.ttypes import StreamType, Stream, AcquisitionDevice
from smac.modules import base
from smac.conf import settings
import re

from sender import OutgoingFileTransfer
from recorder import AcquisitionManager

class FilesystemRecorder(base.ModuleBase):
    """
    A special recorder implementation to simulate the recording by importing the
    streams from files on the filesystem.
    
    This recorder implementation simulates a device for each found folder and a 
    stream for each movie/sound file found inside it.
    
    The search for the folder to emulate are done in the folder specifed by the
    respective configuration directive.
    """
    
    implements(Recorder.Iface)
    
    def __init__(self, *args, **kwargs):
        super(FilesystemRecorder, self).__init__(*args, **kwargs)
        
        self.devices = None
        self.streams = {}
        
        self.sessions = {}
        """A dictionary holding all currently configured acquisition sessions."""
        
        self.basedir = realpath(settings.devices_root)
        
        dirs = (d for d in listdir(self.basedir) if isdir(join(self.basedir, d)))
        devices = [AcquisitionDevice(id=d.lower(), name=d) for d in dirs]
        
        for d in devices:
            self.streams[d.id] = (d.name, {})
            d.streams = []
            for f in listdir(join(self.basedir, d.id)):
                id = f.lower()
                id = re.sub(r'[^0-9a-z]+', '-', id)
                stream = Stream(id=id, type=StreamType.MUXED)
                self.streams[d.name][1][id] = f
                d.streams.append(stream)
        
        self.devices = devices
    
    @defer.inlineCallbacks
    def configure_session(self, session):
        print "Configuring module for session {0}".format(session.id)
        
        self.sessions[session.id] = handler = AcquisitionManager(self, session)
        
        address = Address(routing_key=session.id)
        
        yield self.amq_server(address, SessionListener, handler, AcquisitionManager.queues)
    
    def acquisition_devices(self):
        return self.devices
    
    def setup_session(self, id, setup):
        from smac.acquisition import AcquisitionSetup
        setup = AcquisitionSetup(setup)
        
        assert setup.is_valid()
        
        self.sessions[id] = setup
        
        for r, d, s in setup.streams(self.address):
            path = join(self.basedir, self.streams[d][0], self.streams[d][1][s])
            destinations = setup.archivers((r, d, s))
            
            transfer = OutgoingFileTransfer(path, path)
            
            self.task_register.add(transfer)
            
            def finish(result):
                print "Finished"
                import pprint
                pprint.pprint(result)
            
            transfer.start(self.amq_service.parent.parent, destinations)
            
            transfer.finish().addCallback(finish)
    
