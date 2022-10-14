import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))

import datetime
import json
import logging
import time

# from matplotlib import pyplot as plt

import numpy
import opteryx

from opteryx.managers.kvstores import InMemoryKVStore
cache = InMemoryKVStore(size=25)


def time_function(func, cycles:int=1000, *args):
    # monotonic_ms has the lowest overhead
    timings = [0] * cycles
    for index in range(cycles):
        start = time.monotonic_ns()
        func(*args)
        timings[index] = time.monotonic_ns() - start
        print(".", end="", flush=True)
    print()
    return numpy.array(timings, dtype=numpy.int64)

def execute_query(statement):
    conn = opteryx.connect(cache=cache)
    cur = conn.cursor()
    cur.execute(statement)
    # this should be the fastest way to force the query to exec to completion
    cur.arrow()
    #print(cur.stats)

def reject_outliers(data, m = 3):
    d = numpy.abs(data - numpy.median(data))
    mdev = numpy.median(d)
    s = d/mdev if mdev else 0.
    return data[s<m]

def log_results(results):
    logging.basicConfig(filename="performance.jsonl",
            filemode='a',
            format='%(message)s',
            level=logging.INFO)

    logging.info(json.dumps(results))

def show_results(results):
    print()
    header_len = max([len(k) for k in results.keys()])
    for k, v in results.items():
        print(f"{k.ljust(header_len)} : {v}")

if __name__ == "__main__":

    from opteryx.connectors import GcpCloudStorageConnector
    from opteryx.connectors import AwsS3Connector

    opteryx.register_store("mabel_data", GcpCloudStorageConnector)
    opteryx.register_store("opteryx", AwsS3Connector)

    # parquet
    # opteryx.bulk
    # mabel_data

    CYCLES = 100
    SQL = "SET enable_optimizer = true; SELECT COUNT(*) FROM $satellites WITH(NO_PARTITION) WHERE NOT id <> 25"
    #SQL = "SELECT random() FROM $satellites -- GROUP BY planetId"
    #SQL = "SELECT * FROM FAKE(10, 10)"

    r = time_function(execute_query, CYCLES, SQL)
    min_time, max_time = r.min(), r.max()

    q25, p50, q75, p95, p99 = numpy.percentile(r, [25, 50, 75, 95, 99])
    bin_width = 2 * (q75 - q25) * len(r) ** (-1/3)
    bins = max(round((r.max() - r.min()) / bin_width), 5)

    # Creating histogram
#    fig, ax = plt.subplots(figsize =(10, 7))

    event = {
        "timestamp": datetime.datetime.now().isoformat(),
        "version": opteryx.__version__,
        "query": SQL,
        "cycles": CYCLES,
        "total": sum(r)/1e9,
        "mean": numpy.mean(r)/1e9,
        "min": numpy.min(r)/1e9,
        "max": numpy.max(r)/1e9,
        "iqr": (q75-q25)/1e9,
        "p50": p50/1e9,
        "p95": p95/1e9,
        "p99": p99/1e9,
        "std_dev": numpy.std(r)/1e9
        }

    log_results(event)
    show_results(event)

#    plt.hist(r, bins=bins)
    # Show plot
    #plt.show()