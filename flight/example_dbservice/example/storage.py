#!/usr/bin/env python

# ------------------------------
# License

# Copyright 2023 Aldrin Montana
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# ------------------------------
# Module Docstring
"""
A module of backend integrations to storage (local filesystem or data management systems).
These integrations can be used by flight services (see `services.py`) to serve data and
handle data processing requests.

At the moment, there is a single database example that interfaces with duckdb and a single
filesystem example that uses a local filesystem and assumes CSV files. This is mostly to
illustrate a few of the integration points.
"""


# ------------------------------
# Dependencies

# >> Standard libs
import sys
import hashlib

from pathlib import Path
from typing  import Any

# >> Third-party libs
import duckdb
import pyarrow
import pyarrow.types

from pyarrow import Schema, Table
from pyarrow.lib import DataType


# ------------------------------
# Functions

def ConvertListToType(input_list: list[str], dtype: DataType) -> list[Any]:
    """
    A function that converts all values in a list to a python type based on the given
    :dtype:.
    """

    # check for str type first since it requires no work
    if   pyarrow.types.is_string(dtype):   return                 input_list
    elif pyarrow.types.is_integer(dtype):  return list(map(int  , input_list))
    elif pyarrow.types.is_floating(dtype): return list(map(float, input_list))

    # we don't know what to do with the type
    return input_list


# ------------------------------
# Classes

class LocalFS:
    """
    Implements convenience functions to retrieve Arrow data (Tables) from files in the
    local filesystem.

    For example usage see `SampleTableFromFS()`.
    """

    @classmethod
    def RootedAt(cls, root_dirpath: Path):
        """ Convenience builder function. """
        return cls(root_dirpath)

    def __init__(self, working_dir, **kwargs):
        """
        Initialize an instance that is rooted at the provided :working_dir:.

        :working_dir: is expected to be like a `pathlib.Path`.
        """

        super().__init__(**kwargs)
        self.__working_dir = working_dir

    def TableFromCSV(self, data_fpath: Path, data_schema: Schema) -> Table:
        """
        Convenience function that creates an arrow table from :data_fpath:.

        :data_fpath: should be a string-like path or a `pathlib.Path` and it is assumed
        that it is relative to self.__working_dir and that the first line of the file
        contains column headers.
        """

        # Use a comma-delimiter for rows in CSV format
        field_delim = ','

        # read data from the given file
        with open(self.__working_dir / data_fpath) as data_handle:

            # read column headers and validate the column count.
            col_names = next(data_handle).split(field_delim)
            col_count = len(col_names)
            assert len(data_schema) == col_count

            # accumulate data into python lists
            data_by_col = [ [] for _ in range(col_count) ]

            # for each row
            for line in data_handle:
                fields = line.strip().split(field_delim)

                # for each column
                for ndx, field_val in enumerate(fields):
                    data_by_col[ndx].append(field_val)

        # convert types if needed, then might as well convert to pyarrow too
        converted_data = [
            pyarrow.array(
                 ConvertListToType(col_data, data_schema[col_ndx].type)
                ,data_schema[col_ndx].type
            )
            for col_ndx, col_data in enumerate(data_by_col)
        ]

        # construct the table and return it
        return Table.from_arrays(converted_data, schema=data_schema)


class DuckDBMS:

    @classmethod
    def Exists(cls, db_filepath):
        """ Checks if the database at :db_filepath: exists. """
        return Path(db_filepath).is_file()

    @classmethod
    def InFile(cls, db_filepath, mutable=True):
        """ Initialize a DuckDBMS instance as a new file-backed DuckDB database. """
        return cls(duckdb.connect(database=db_filepath, read_only=(not mutable)))

    @classmethod
    def InMemory(cls, arrow_src=None):
        """
        Initialize a DuckDBMS instance as a new in-memory DuckDB database.

        If :arrow_src: is specified, all queries are directed to it instead of an in-mem
        database. It is assumed that :data_src: is a valid Arrow object, see:
            https://duckdb.org/docs/guides/python/sql_on_arrow
        """

        return cls(duckdb.connect(database=':memory:'), data_src=arrow_src)

    def __init__(self, db_conn, data_src=None, **kwargs):
        """
        Initialize the DuckDB database given :db_conn:. It is expected that this is only
        called by a builder classmethod (such as `InMemory` or `InFile`):
            db = DuckDBMS.InMemory()

        If :data_src: is specified, all queries are directed to it instead of an in-mem
        database. It is assumed that :data_src: is a valid Arrow object, see:
            https://duckdb.org/docs/guides/python/sql_on_arrow
        """

        super().__init__(**kwargs)

        self.__data_src   = data_src
        self.__dbconn     = db_conn
        self.__table_list = []

    def LoadExtensionSubstrait(self) -> None:
        self.__dbconn.install_extension('substrait')
        self.__dbconn.load_extension('substrait')

    def ShowTables(self, use_cache=True) -> list[str]:
        """
        Get a list of all tables in the database. If :use_cache: is True (default), then
        do not query the database if we already have a list of all tables.
        """

        if not use_cache or not self.__table_list:
            self.__table_list = self.__dbconn.execute('SHOW TABLES').fetchall()

        return self.__table_list

    def TableExists(self, name: str, use_cache=True) -> bool:
        """
        Gets the table list using `ShowTables`. If :use_cache: is True (default), then
        `ShowTables` will not query the database if it already has a list of all tables.

        :returns: True if the table, :name:, is in the list of tables.
        """

        return name in self.ShowTables(use_cache)

    def CreateTable(self, name: str, arrow_table: Table, replace: False):
        """
        Convenience method for creating a database table. If this db instance was
        created from an Arrow object, this method will fail (return None).
        """

        # TODO: figure out a better way to propagate failure for this scenario.
        if self.__data_src is not None: return

        replace_clause = '' if not replace else 'OR REPLACE'
        self.__dbconn.execute(f'''
            CREATE {replace_clause} TABLE {name}
                      AS SELECT *
                           FROM arrow_table
        ''')

    def InsertData(self, name: str, arrow_table: Table):
        """ Convenience method for inserting many tuples into a table. """

        self.__dbconn.execute(f'''
            INSERT INTO {name}
                 SELECT *
                   FROM arrow_table
        ''')

    def ScanData(self, name: str, limit=20):
        """ Convenience method to scan a table name. """

        self.__dbconn.execute(f'''
            SELECT *
            FROM   {name}
            LIMIT  {limit}
        ''')

        return self.__dbconn.fetchall()

    def QueryData(self, query_str):
        """
        Convenience method to execute the query string against a singleton db
        connection.
        """

        self.__dbconn.execute(query_str)
        return self.__dbconn.fetchall()

    def ExecuteSubstrait(self, plan_msg: bytes):
        """
        Convenience method to execute a serialized substrait plan against the db.
        """

        result = self.__dbconn.from_substrait(proto=plan_msg)
        return result.fetchall()

