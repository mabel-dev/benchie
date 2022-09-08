import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))

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
    # this should be the fastest way to force the query to exec to completion
    cur.to_arrow()

def reject_outliers(data, m = 3):
    d = numpy.abs(data - numpy.median(data))
    mdev = numpy.median(d)
    s = d/mdev if mdev else 0.
    return data[s<m]

if __name__ == "__main__":

    CYCLES = 250
    SQL = "SELECT * FROM data"

    r = time_function(execute_query, CYCLES, SQL)
    min_time, max_time = r.min(), r.max()

    # remove outliers
    r = reject_outliers(r)

    # Creating histogram
    fig, ax = plt.subplots(figsize =(10, 7))
 
    q25, q75 = numpy.percentile(r, [25, 75])
    bin_width = 2 * (q75 - q25) * len(r) ** (-1/3)
    bins = round((r.max() - r.min()) / bin_width)

    print(f"""
Query  : {SQL}
Cycles : {CYCLES}
Total  : {sum(r)/1e9}
Mean   : {numpy.mean(r)/1e9}
Min    : {numpy.min(r)/1e9} ({min_time/1e9})
Max    : {numpy.max(r)/1e9} ({max_time/1e9})
IQR    : {(q75-q25)/1e9}
StdDev : {numpy.std(r)/1e9}
    """)

    plt.hist(r, bins=bins)
    # Show plot
    plt.show()