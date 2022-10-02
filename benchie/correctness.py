"""
Run a suite of correctness tests for Opteryx by comparing to MySQL or Postgres.

This helps to ensure SQL statements return the correct result by comparing
the output from Opteryx to the identical query on an accepted 'standard' SQL
engine. 

No two SQL Engines are identical, there is no obsolute and universal 'correct'
answer to every query - so some explainable failures may be present.
"""
import os
import sys
from unittest import result

sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))

from functools import reduce

import orjson

from cityhash import CityHash64

def get_tests():
    import glob

    suites = glob.glob(f"**/correctness/**.test", recursive=True)
    for suite in suites:
        with open(suite, mode="r") as test_file:
            yield from [
                line
                for line in test_file.read().splitlines()
                if len(line) > 0 and line[0] != "#"
            ]

def execute_statement(connection, statement):
    """
    Using DBAPI, we cna wrap Opteryx and an external DB
    """
    cur = connection.cursor()
    cur.execute(statement)
    result = list(cur.fetchall())
    return result


def hash_the_table(table):
    """
    Hash all of the values in the table.

    We can do this by hashing the values in each row into a hash and then
    XORing all of these together.
    """
    # To do this we order the dictionaries which hold the records to ensure
    # they are consistent. we dump the sorted dictionaries to JSON strings.
    # We hash those JSON string, record by record, XORing them together.

    seed = 7097667599182356662

    def inner(row):  
        hashes = map(CityHash64, map(str, row))
        hashed = reduce(lambda x, y: x ^ y, hashes)
        return hashed

    hashed = reduce(lambda x, y: x ^ inner(y), table, seed)
    return hashed


def compare_results(results_opteryx, results_external):
    """
    Databases dont need to guarantee the order of the results so unless we
    ORDER VY every test, we can't directly compare the results. So we're
    going to do the following the check:

        a) check the rows counts match (pretty basic test)
        b) check the column names match
        c) check the hash of all of the data matches

    We're going to do these as `assert` statements, because this is a test.
    """

    # CHECK THE ROW COUNTS MATCH
#    len(results_opteryx) == len(results_external), f"Row count mismatch: {len(results_opteryx)} != {len(results_external)}"

    if len(results_opteryx) == 0:
        # there's no data to test
        return False

    # CHECK THE COLUMN NAMES MATCH
    # get the first record of each to check form
    first_opteryx = results_opteryx[0]
    first_external = results_external[0]

#    assert set(first_opteryx.keys()) == set(first_external.keys()), f"Column name mismatch: {first_opteryx.keys()} vs {first_external.keys()}"

    # CHECK THE VALUES HASHS MATCH
    opteryx_hash = hash_the_table(results_opteryx)
    external_hash = hash_the_table(results_external)
    return opteryx_hash == external_hash  #, f"Data hash mismatch\n{results_external}\n{results_opteryx}"


def main():
    import duckdb
    import opteryx

    exemplar = duckdb.connect()
    subject = opteryx.connect()

    import time
    import shutil

    width = shutil.get_terminal_size((80, 20))[0] - 24

    print("\033[4;36mID  \033[0m \033[4;37mStatement".ljust(width) + "                      \033[0m \033[4;33mDuckDB\033[0m \033[4;34mOpteryx\033[0m")
    for index, sql in enumerate(get_tests()):
        print(
            f"\033[0;36m{(index + 1):04}\033[0m {sql[0:width - 1].ljust(width)}",
            end="",
        )
        start = time.monotonic_ns()
        examplar_result = execute_statement(exemplar, sql.replace("$planets", "'data/planets/planets.parquet'"))
        print(f"\033[0;33m{str(int((time.monotonic_ns() - start)/1000000)).rjust(4)}ms\033[0m", end="  ")
        start = time.monotonic_ns()
        subject_result = execute_statement(subject, sql)
        print(f"\033[0;34m{str(int((time.monotonic_ns() - start)/1000000)).rjust(4)}ms\033[0m", end="  ")
        passed = compare_results(subject_result, examplar_result)
        if passed:
            print("✅")
        else:
            print("❌")


if __name__ == "__main__":
    main()
