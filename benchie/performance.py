import time

from matplotlib import pyplot as plt

import numpy
import opteryx


def time_function(func, cycles:int=1000, *args):
    timings = [0] * cycles
    for index in range(cycles):
        start = time.perf_counter_ns()
        func(*args)
        timings[index] = time.perf_counter_ns() - start
    return numpy.array(timings, dtype=numpy.int32)

def execute_query(statement):
    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute(statement)
    [i for i in cur.fetchall()]

def reject_outliers(data, m = 3):
    d = numpy.abs(data - numpy.median(data))
    mdev = numpy.median(d)
    s = d/mdev if mdev else 0.
    return data[s<m]

if __name__ == "__main__":


    r = time_function(execute_query, 1000, "SELECT * FROM $planets;")
    min_time, max_time = r.min(), r.max()

    # remove outliers
    r = reject_outliers(r)

    # Creating histogram
    fig, ax = plt.subplots(figsize =(10, 7))
 
    q25, q75 = numpy.percentile(r, [25, 75])
    bin_width = 2 * (q75 - q25) * len(r) ** (-1/3)
    bins = round((r.max() - r.min()) / bin_width)
    print("Freedman–Diaconis number of bins:", bins)
    plt.hist(r, bins=bins);
    
    # Show plot
    plt.show()