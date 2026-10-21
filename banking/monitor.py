import threading
import time
import psutil

class ResourceMonitor:
    """Sample driver Python + child processes. Executors need separate container metrics."""
    def __init__(self):
        self.samples=[]
        self.done=threading.Event()
    def start(self):
        def sample():
            root=psutil.Process()
            known={}
            while not self.done.is_set():
                rss=0;cpu=0
                for process in [root]+root.children(recursive=True):
                    try:
                        process=known.setdefault(process.pid,process)
                        rss+=process.memory_info().rss
                        cpu+=process.cpu_percent(None)
                    except (psutil.NoSuchProcess,psutil.AccessDenied):pass
                self.samples.append({'time':time.time(),'rss_bytes':rss,'cpu_percent':cpu})
                self.done.wait(1)
        self.thread=threading.Thread(target=sample,daemon=True);self.thread.start();return self
    def stop(self):
        self.done.set();self.thread.join()
        return {'scope':'driver Python and child processes; excludes remote executors','sample_interval_seconds':1,'samples':len(self.samples),'peak_rss_bytes':max((x['rss_bytes'] for x in self.samples),default=0),'peak_cpu_percent':max((x['cpu_percent'] for x in self.samples),default=0),'series':self.samples}
