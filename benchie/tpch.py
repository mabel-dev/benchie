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

sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))



def get_tests():
    import glob

    suites = glob.glob(f"**/tpch/**.sql", recursive=True)
    for suite in suites:
        with open(suite, mode="r") as test_file:
            yield suite, test_file.read()

def execute_statement(connection, statement):
    """
    Using DBAPI, we cna wrap Opteryx and an external DB
    """
    cur = connection.cursor()
    cur.execute(statement)
    result = cur.arrow()
    return result


def main():
    import opteryx

    subject = opteryx.connect()

    import time
    import shutil

    width = shutil.get_terminal_size((80, 20))[0] - 16

    print(
        "\033[4;36mID  \033[0m \033[4;37mTest".ljust(width)
        + "                      \033[0m \033[4;34mResult    \033[0m"
    )
    for index, (name, sql) in enumerate(get_tests()):
        print(
            f"\033[0;36m{(index + 1):04}\033[0m {name[0:width - 1].ljust(width)}",
            end="",
        )
        start = time.monotonic_ns()
        try:
            subject_result = execute_statement(subject, sql)
            passed = True
        except:
            passed = False
        print(
            f"\033[0;33m{str(int((time.monotonic_ns() - start)/1000000)).rjust(4)}ms\033[0m",
            end="  ",
        )
        if passed:
            print("✅")
        else:
            print("❌")


if __name__ == "__main__":
    main()
