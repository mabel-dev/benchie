import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))

import sqlite3


TYPES = {
    "int64": "INTEGER",
    "string": "TEXT",
    "double": "REAL", 
}

def _derive_create(name, parq):
    """ derive the CREATE statement for this parquet file """
    schema = parq.schema
    sql = f"CREATE TABLE {name} (\n"
    fields = []
    for field in schema:
        fields.append(f"\t{field.name.ljust(32)} {TYPES.get(field.type, 'TEXT')}")
    sql += ",\n".join(fields)
    sql += "\n); "
    return sql

def _insert_values(name, parq):
    
    def _escape(values):
        for value in values:
            if isinstance(value, str):
                yield f'"{value}"'
            elif value is None:
                yield 'NULL'
            else:
                yield str(value)

    for row in parq.to_pylist():
        sql = f"INSERT INTO {name} ({', '.join(parq.column_names)})\nVALUES\n"
        sql += f"\t({', '.join(_escape(row.values()))})"

        yield sql


def _get_dataset(dataset):
    import opteryx.samples

    if dataset == "planets":
        return opteryx.samples.planets()
    

if __name__ == "__main__":

    exemplar_datafile = "data/sqlite/exemplar.sqlite"

    datasets = ("planets",)

    try:
        os.remove(exemplar_datafile)
    except OSError:
        pass

    for name in datasets:

        dataset = _get_dataset(name)
        create_statement = _derive_create(name, dataset)

        conn = sqlite3.connect(database=exemplar_datafile)
        conn.execute(create_statement)
        conn.commit()
        conn.close()

        for statement in _insert_values(name, dataset):
            conn = sqlite3.connect(database=exemplar_datafile)
            conn.execute(statement)
            conn.commit()
            conn.close()
