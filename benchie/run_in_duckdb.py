"""
Run a suite of correctness tests for Opteryx by comparing to MySQL or Postgres.

This helps to ensure SQL statements return the correct result by comparing
the output from Opteryx to the identical query on an accepted 'standard' SQL
engine. 

No two SQL Engines are identical, there is no obsolute and universal 'correct'
answer to every query - so some explainable failures may be present.
"""
import datetime
import decimal

from functools import reduce

from cityhash import CityHash64


def execute_statement(connection, statement):
    """
    Using DBAPI, we cna wrap Opteryx and an external DB
    """
    cur = connection.cursor()
    cur.execute(statement)
    return cur.arrow()


def main():
    import duckdb

    exemplar = duckdb.connect()

    import time
    import shutil

    width = shutil.get_terminal_size((80, 20))[0] - 24

    sql = "SELECT CASE id WHEN 3 THEN 'Earth' WHEN 1 THEN 'Mercury' ELSE 'Elsewhere' END FROM $planets;"

    start = time.monotonic_ns()
    exemplar_sql = sql
    exemplar_sql = exemplar_sql.replace("$planets", "'data/planets/planets.parquet'")
    exemplar_sql = exemplar_sql.replace(
        "$astronauts", "'data/astronauts/astronauts.parquet'"
    )
    examplar_result = execute_statement(exemplar, exemplar_sql)
    print(
        f"\033[0;33m{str(int((time.monotonic_ns() - start)/1000000)).rjust(4)}ms\033[0m",
        end="  ",
    )
    start = time.monotonic_ns()

    print(f"\033[0;DUCKDB ({len(examplar_result)})\033[0m")
    print(examplar_result)


if __name__ == "__main__":
    main()
