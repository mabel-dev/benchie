import time
import numpy
 
loops = 1000000
 
def measure(timefunc):
        h = []
        for l in range(0, loops):
                start = time.monotonic_ns()
                dummy = timefunc()
                h.append(1000000000*(time.monotonic_ns() - start))
        return h
 
if __name__ == '__main__':
        for func in (time.monotonic_ns, time.monotonic, time.perf_counter, time.process_time, time.time):
                print("Histogram for {}".format(func))
                hist = measure(func)

                print(func, [f"{n} {numpy.percentile(hist, n)}" for n in [50, 95, 99, 99.99]])