"""
   metrics
"""

from abc import abstractmethod
from datetime import datetime
import threading
import time

from utils import get_memory_info

#############################################
# classes
#
class PeriodicTimer(object):
    def __init__(self, interval):
        self._interval = interval
        self._flag = 0
        self._cv = threading.Condition()

    def start(self):
        t = threading.Thread(target=self.run)
        t.daemon = True
        t.start()

    def run(self):
        while True:
            time.sleep(self._interval)
            with self._cv:
                self._flag ^= 1
                self._cv.notify_all()

    def wait_for_tick(self):
        with self._cv:
            last_flag = self._flag
            while last_flag == self._flag:
                self._cv.wait()


#############################################
# metricses
class MetricsBase(threading.Thread):
    def __init__(self, interval=60, skip_first=False):
        super().__init__()

        self.interval = interval
        self._skip_first = skip_first

#        self.daemon = True
        self._timer = PeriodicTimer(self.interval)
        self._exit = False

    def _init(self):
        pass

    def _main(self):
        pass

    def run(self):
        """ main loop """
        self._init()
        self._timer.start()

        if self._skip_first:  # sleep one interval to get delta values
            self._timer.wait_for_tick()

        while True:
            self._main()
            self._timer.wait_for_tick()

    def stop(self):
        """ stop the data sensor, try to exit gracefully """
        self._exit = True
        self.join(self.interval)


class MemoryMetrics(MetricsBase):
    M_NAME = "m_metrics"
    def __init__(self, interval=60, chunk_size=10000):
        super().__init__(interval, False)
        self._fileno = 0
        self._lineno = 0
        self._chunk_size = chunk_size
        self._ofd = open(f'{self.M_NAME}_{self._fileno}.csv', 'w')
        print('timestamp,mem_total,mem_avail,mem_avail_percent',
              file=self._ofd)

    def _main(self):
        meminfo = get_memory_info()
        mm = {'MemTotal': meminfo['MemTotal'], 
              'MemFree': meminfo['MemFree'],
              'Buffers': meminfo['Buffers'],
              'Cached': meminfo['Cached'],
              }
        mm['Available'] = (mm['MemFree'] + mm['Buffers'] + mm['Cached']) 
        print(','.join([f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                        f'{mm["MemTotal"]}',
                        f'{mm["Available"]}',
                        f'{mm["Available"] / mm["MemTotal"] * 100:.3f}']),
              file=self._ofd)
        self._lineno += 1
        if self._lineno % 10 == 0:
            self._ofd.flush()
        if self._lineno == self._chunk_size:
            self._ofd.close()
            self._fileno += 1
            self._lineno = 0
            self._ofd = open(f'{self.M_NAME}_{self._fileno}.csv', 'w')
            print('timestamp,mem_total,mem_avail,mem_avail_percent',
                  file=self._ofd)


if __name__ == "__main__":
    metric = MemoryMetrics(1, 10)
    metric.start()
