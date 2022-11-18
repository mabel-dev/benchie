import time

from functools import wraps

import numpy

loops = 100000


def measure(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        h = []
        for l in range(0, loops):
            start = time.monotonic_ns()
            func(*args, **kwargs)
            h.append((time.monotonic_ns() - start) / 1e3)
        print(
            "".join(
                [
                    f"{n:2}% {numpy.percentile(h, n):8.4f}ms, "
                    for n in [50, 95, 99, 99.99]
                ]
            ),
            f"{loops} cycles of {func.__name__}",
        )

    return wrapper


if __name__ == "__main__":

    import os
    import sys

    sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))

    import random

    def noop(n):
        return n


    # for func in (time.monotonic_ns, time.monotonic, time.perf_counter, time.process_time, time.time):
    for func in (lambda x: random.random(), os.urandom, random.getrandbits):
        measure(func)(8)

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

RANDOM.GETRANDBITS WINS
50%   0.1840ms, 95%   0.2570ms, 99%   0.3270ms, 99.99%   5.0570ms,  1000000 cycles of random.getrandbits(16)
50%   1.0330ms, 95%   1.1250ms, 99%   1.4220ms, 99.99%  18.3190ms,  1000000 cycles of os.urandom(2)
50%   7.0530ms, 95%   8.2820ms, 99%  13.9220ms, 99.99% 106.2970ms,  1000000 cycles of numpy.random.bytes(2)

MAP WINS
50%  86.8710ms, 95% 101.9470ms, 99% 120.6632ms, 99.99% 615.2880ms,  100000 cycles of comprehension
50%  61.4150ms, 95%  75.3643ms, 99%  90.9310ms, 99.99% 435.3975ms,  100000 cycles of list(map)
"""
