import time

from functools import wraps

import numpy
 
loops = 1000000
 
def measure(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        h = []
        for l in range(0, loops):
            start = time.monotonic_ns()
            func(*args, **kwargs)
            h.append((time.monotonic_ns() - start) / 1e3)
        print("".join([f"{n:2}% {numpy.percentile(h, n):8.4f}ms, " for n in [50, 95, 99, 99.99]]),
        f"{loops} cycles of {func.__name__}")
    return wrapper
 
if __name__ == '__main__':
    for func in (time.monotonic_ns, time.monotonic, time.perf_counter, time.process_time, time.time):
        measure(func)()

"""
CITYHASH WINS
50%   0.1330ms, 95%   0.1510ms, 99%   0.1770ms, 99.99%   5.2350ms,  1000000 cycles of CityHash32
50%   0.1330ms, 95%   0.2080ms, 99%   0.2780ms, 99.99%   8.9960ms,  1000000 cycles of FarmHash32
50%   0.2380ms, 95%   0.2730ms, 99%   0.3050ms, 99.99%   7.2720ms,  1000000 cycles of xxh32
50%   0.1430ms, 95%   0.1890ms, 99%   0.2640ms, 99.99%   9.2040ms,  1000000 cycles of hash

MONOTONIC_NS WINS
50%   0.1290ms, 95%   0.1610ms, 99%   0.1750ms, 99.99%   1.4130ms,  1000000 cycles of monotonic_ns
50%   0.1410ms, 95%   0.1850ms, 99%   0.2270ms, 99.99%   1.6000ms,  1000000 cycles of monotonic
50%   0.1630ms, 95%   0.2480ms, 99%   0.2990ms, 99.99%   8.4830ms,  1000000 cycles of perf_counter
50%   0.3120ms, 95%   0.3800ms, 99%   0.5320ms, 99.99%  16.6380ms,  1000000 cycles of process_time
50%   0.1630ms, 95%   0.1890ms, 99%   0.2270ms, 99.99%   1.7080ms,  1000000 cycles of time
"""