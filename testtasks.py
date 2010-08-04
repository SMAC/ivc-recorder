from zope.interface import implements

from smac.tasks import Task, ITask
from smac.api.ttypes import TaskStatus, TaskType

from twisted.python import log
from twisted.internet import reactor

from datetime import timedelta

#@register('post_startup')
#def test(self):
#    """
#    This method is here only to execute some test tasks to show in the
#    controller web interface.
#    
#    It can be removed at any time in the future.
#    """
#    from testtasks import AcquisitionSession, FileUpload
#    
#    undet = AcquisitionSession(status=0, id=12,
#            status_text='Recording from three devices')
#    
#    det = FileUpload(status=0, id=1234, status_text="Example test task")
#    
#    self.add_task(det)
#    self.add_task(undet)

class AcquisitionSession(object):
    implements(ITask)
    
    type = TaskType.UNDETERMINED
    
    def __init__(self, **kwargs):
        for k, v in kwargs.iteritems():
            setattr(self, k, v)
            
    def add_observer(self, observer):
        pass
    
class FileUpload(Task):
    speed = 50
    
    def __init__(self, **kwargs):
        super(FileUpload, self).__init__(**kwargs)
        self.remaining = timedelta(0, self.speed)
        
        self.status_text = "Initializing..."
        
        reactor.callLater(3, self.progress)
    
    def progress(self):
        import random
        
        if not self.completed:
            self.completed = 0
        
        self.completed += 1
        self.remaining -= timedelta(0, self.speed/100.0)
        
        if (self.completed == 100):
            self.status = TaskStatus.COMPLETED
            self.status_text = 'Finished launching services.'
            self.notify_observers()
            
            self.completed = 0
            self.remaining = timedelta(0, self.speed)
            self.status = TaskStatus.RUNNING
            reactor.callLater(5, self.progress)
        else:
            self.status_text = random.choice((
                'Hi, I\'m only a simple test task',
                'I will cycle through different states',
                'Completely random!'
            ))
            
            self.notify_observers()
            log.msg(self.status_text)
            reactor.callLater(self.speed/100.0, self.progress)