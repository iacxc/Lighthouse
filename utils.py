

def get_memory_info():
    """
       get memory information from /proc/meminfo
    """
    mem_info = {}
    with open('/proc/meminfo') as fhmem:
        for line in fhmem:
            fields = line.split(':')
            key = fields[0]
            value = fields[1].strip().split()[0]
            mem_info[key] = int(value)

    return mem_info
