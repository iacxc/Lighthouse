import os
import sched
import time


CLK_TCK = os.sysconf('SC_CLK_TCK')


def get_cpu_usage():
    """
       Return the summary cpu stat data
    """
    cpu_stats = {}
    with open('/proc/stat') as fd:
        for line in fd:
            if not line.startswith('cpu'):
                continue
            fields = line.split()
            cpu = fields[0]

            stat = {
                'utime' : float(fields[1]) * 1000 / CLK_TCK,
                'nice'  : float(fields[2]) * 1000 / CLK_TCK,
                'stime' : float(fields[3]) * 1000 / CLK_TCK,
                'idle'  : float(fields[4]) * 1000 / CLK_TCK,
                'iowait': float(fields[5]) * 1000 / CLK_TCK,
                'irq':    float(fields[6]) * 1000 / CLK_TCK,
                'softirq':float(fields[7]) * 1000 / CLK_TCK,
                'steal' : 0,
                'guest' : 0
            }

            stat['total'] = sum([stat['utime'], 
                                 stat['nice'],
                                 stat['stime'],
                                 stat['idle'],
                                 stat['iowait'],
                                 stat['irq'],
                                 stat['softirq']])

            if len(fields) > 8:
                stat['steal'] = float(fields[8]) * 1000 // CLK_TCK

            if len(fields) > 9:
                stat['guest'] = float(fields[9]) * 1000 // CLK_TCK

            cpu_id = -1 if cpu == 'cpu' else int(cpu[3:])

            cpu_stats[cpu_id] = stat

    return cpu_stats


def top(cpu_stats=None, each_cpu=False):
    if cpu_stats is None:
        cpu_stats = get_cpu_usage()
    for cpu_id, stat in cpu_stats.items():
        if each_cpu ^ (cpu_id == -1):
            if cpu_id == -1:
                print('%Cpu  :', end=' ')
            else:
                print(f'%Cpu{cpu_id} :', end=' ')
            print(f'{stat["utime"]/stat["total"] * 100:4.1f} us', end=', ')
            print(f'{stat["stime"]/stat["total"] * 100:4.1f} sy', end=', ')
            print(f'{stat["nice"]/stat["total"] * 100:4.1f} ni', end=', ')
            print(f'{stat["idle"]/stat["total"] * 100:4.1f} id')


class ShowTop():
    def __init__(self, inc=2):
        self.inc = inc
        self.schd = sched.scheduler(time.time, time.sleep)

    def show(self, inc):
        top(each_cpu=True)
        self.schd.enter(inc, 0, self.show, (inc,))

    def run(self):
        self.schd.enter(0, 0, self.show, (self.inc,))
        self.schd.run()
