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
A module of functions for bootstrapping instances on sample data. These functions should
(or could) serve as examples/recipes.
"""


# ------------------------------
# Dependencies

# >> Standard libs
from pathlib import Path

# >> Third-party libs
import pyarrow

# >> Internal libs
from example.storage import LocalFS, DuckDBMS


# ------------------------------
# Functions

# >> Functions that primarily interface with the file system

def CreateSampleFileService():
    """
    A function that shows how to initialize a LocalFS instance using a working directory.
    """

    # hard-coded variables for sample data
    sample_dirpath = Path('resources') / 'data'

    return LocalFS.RootedAt(sample_dirpath)


def ReadArrowTableFromFS():
    """
    A function that shows how to read an Arrow Table from a single CSV file using a local
    file system and a known schema.
    """

    # hard-coded variables for sample data
    sample_fpath   = 'sampledata.csv'
    sample_schema  = pyarrow.schema([
         pyarrow.field('gene_id'   , pyarrow.string())
        ,pyarrow.field('cell_id'   , pyarrow.string())
        ,pyarrow.field('expression', pyarrow.float32())
    ])

    local_fs = CreateSampleFileService()
    return local_fs.TableFromCSV(sample_fpath, sample_schema)


def ReadArrowBatchesFromFS():
    """
    A function that shows how to read an Arrow Table as batches from many CSV files using
    a local file system and a known schema.
    """

    # hard-coded variables for sample data
    sample_schema  = pyarrow.schema([
         pyarrow.field('gene_id'   , pyarrow.string())
        ,pyarrow.field('cell_id'   , pyarrow.string())
        ,pyarrow.field('expression', pyarrow.float32())
    ])

    # return a list of Tables each with the same schema and each a slice of a larger table
    local_fs = CreateSampleFileService()
    return [
        local_fs.TableFromCSV(f'sampledata.{slice_ndx}.csv', sample_schema)
        for slice_ndx in range(4)
    ]


# >> Functions that primarily interface with a DBMS (DuckDB)

def NormalizeTableName(table_name):
    """ A convenience function to change ';' and '/' characters to '_'. """

    return table_name.replace('/', '_').replace(';', '_')


def CreateSampleDBConn():
    """ A function to create a new database from sample data. """

    # initialize the database
    default_datadirpath = Path('resources')   / 'data' / 'db'
    default_datafpath   = default_datadirpath / 'sample-data.duckdb'

    # the `InFile` classmethod creates the database backed by a file
    return DuckDBMS.InFile(str(default_datafpath))


def InitializeSampleTable(duck_dbms, table_name):
    """
    A convenience function that initializes a DuckDB table from sample data.
    """

    # First read the sample data from a CSV file
    table_data = ReadArrowTableFromFS()

    # Then create a table using the given DB connection and table name
    duck_dbms.CreateTable(table_name, table_data, replace=True)


def InitializeSampleTableFromSlices(duck_dbms, table_names):
    # Create many tables using the given DB connection and list of table names
    for table_ndx, table_data in enumerate(ReadArrowBatchesFromFS()):
        duck_dbms.CreateTable(table_names[table_ndx], table_data, replace=True)


def ScanDBTable(duck_dbms, table_names):
    results = []

    for table_name in table_names:
        results.extend(duck_dbms.ScanData(table_name))

    return results


def PrepareDatabase():
    duck_dbms = CreateSampleDBConn()

    # create a single table
    single_tname = 'sampledata'
    InitializeSampleTable(duck_dbms, single_tname)

    # create a table for each slice
    slice_tnames = [f'test_sample_{slice_ndx}' for slice_ndx in range(4)]
    InitializeSampleTableFromSlices(duck_dbms, slice_tnames)

    return duck_dbms


# >> Convenience functions for interacting with substrait

def ReadSampleQueryPlan():
    # define hard-coded variables and validate the file path
    plan_dirpath = Path('resources') / 'examples'
    plan_fpath   = plan_dirpath / 'average-expression.substrait'

    if not plan_fpath.is_file():
        sys.exit(f'Invalid file path for query plan: [{plan_fpath}]')

    # read the serialized substrait plan from a binary file
    with open(plan_fpath, 'rb') as proto_handle:
        plan_msg = proto_handle.read()

    # return the serialized substrait plan
    return plan_msg
