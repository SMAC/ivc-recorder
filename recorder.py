

from twisted.internet import defer

from smac.acquisition import AcquisitionSetup
from smac.sessions.remote import SessionListener
from smac.tasks import Task

from sender import OutgoingFileTransfer

from os.path import join
import ivc


class RecordingTask(Task):
    def __init__(self, parent, session):
        super(RecordingTask, self).__init__(sessid=session.id, parent=parent)
        
        self.session = session
    
    def run(self):
        self.recorder = ivc.IVC4300Recorder()
        return self.recorder.start()
        print "Start recording session {0} (parent task is {1})".format(self.session.id, self.parent)
    
    @defer.inlineCallbacks
    def complete(self):
        msg = yield self.recorder.stop()
        super(RecordingTask, self).complete(msg)


class AcquisitionManager(SessionListener):
    
    def __init__(self, *args, **kwargs):
        super(AcquisitionManager, self).__init__(*args, **kwargs)
        
        self.setup = AcquisitionSetup(self.session.setup)
    
    @defer.inlineCallbacks
    def recording_start(self, task):
        print "Received recording start signal", task
        
        task = RecordingTask(task, self.session)
        yield self.host.task_register.add(task)
        task.start()
    
    @defer.inlineCallbacks
    def archive(self, task):
        for r, d, s in self.setup.streams(self.host.address):
            path = join(self.host.basedir, self.host.streams[d][0], self.host.streams[d][1][s])
            destinations = self.setup.archivers((r, d, s))
            
            transfer = OutgoingFileTransfer(path, path)
            transfer.parent = task
            
            yield self.host.task_register.add(transfer)
            
            def finish(result):
                print "Finished"
                import pprint
                pprint.pprint(result)
            
            transfer.start(self.host.amq_service.parent.parent, destinations)
            transfer.finish().addCallback(finish)
        