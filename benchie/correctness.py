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
    # they are consistent. we dump the sorted dictionaries to JSON strins.
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
    assert len(results_opteryx) == len(results_external), f"Row count mismatch: {len(results_opteryx)} != {len(results_external)}"

    if len(results_opteryx) == 0:
        # there's no data to test
        return

    # CHECK THE COLUMN NAMES MATCH
    # get the first record of each to check form
    first_opteryx = results_opteryx[0]
    first_external = results_external[0]

#    assert set(first_opteryx.keys()) == set(first_external.keys()), f"Column name mismatch: {first_opteryx.keys()} vs {first_external.keys()}"

    # CHECK THE VALUES HASHS MATCH
    opteryx_hash = hash_the_table(results_opteryx)
    external_hash = hash_the_table(results_external)
    assert opteryx_hash == external_hash, f"Data hash mismatch\n{results_external}\n{results_opteryx}"


def main():
    import sqlite3
    import opteryx

    exemplar = sqlite3.connect(database=f"data/sqlite/exemplar.sqlite")
    subject = opteryx.connect()

    tests = [
        "SELECT * FROM $planets ORDER BY id;",
        "SELECT COUNT(*), gravity FROM $planets GROUP BY gravity;",
        ]

    for index, sql in enumerate(tests):
        print(f"{index:4}", sql[0:100].ljust(100), end='')
        examplar_result = execute_statement(exemplar, sql.replace("$", ""))
        subject_result = execute_statement(subject, sql)

        compare_results(subject_result, examplar_result)
        print("✅")


if __name__ == "__main__":
    main()
