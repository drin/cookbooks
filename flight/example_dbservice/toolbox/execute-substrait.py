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
# Dependencies

# >> Standard modules
import sys
import logging

from pathlib import Path

# >> Internal
#   |> functions
from example          import AddConsoleLogHandler
from example.executor import ExecuteSubstrait
from example.recipes  import ReadArrowTableFromFS, ReadArrowBatchesFromFS, PrepareDatabase

#   |> variables
from example import default_loglevel


# ------------------------------
# Module variables

# >> Logging
logger = logging.getLogger(__name__)
logger.setLevel(default_loglevel)
AddConsoleLogHandler(logger)


# ------------------------------
# Functions

def ExecuteWithAcero(plan_msg: bytes):
    """
    A convenience function to execute a serialized substrait plan using Acero.

    Unfortunately the sample query plan uses a coalesce function that is not supported by
    Acero.
    """

    # initialize a dictionary of table names to Arrow Tables
    table_mapping = { 'sampledata': ReadArrowTableFromFS() }
    for slice_ndx, slice_data in enumerate(ReadArrowBatchesFromFS()):
        # NOTE: we use these slice table names to hard code names the sample query uses
        slice_tname = f'test/sample;{slice_ndx}'
        table_mapping[slice_tname] = slice_data

    # define a data provider as a closure that takes a table name and returns a table
    def AceroDataProvider(table_names, expected_schema=None):
        table_name  = ''.join(table_names)
        logger.debug(f'Table requested: [{table_name}]')

        return table_mapping[table_name]

    # then execute the substrait plan using Acero
    from pyarrow.lib import ArrowNotImplementedError
    try:
        return ExecuteSubstrait(AceroDataProvider, plan_msg)

    except ArrowNotImplementedError as exec_err:
        logger.error('Acero unable to execute query:')
        sys.exit(str(exec_err))

def ExecuteWithDuckDB(plan_msg: bytes):
    """
    A convenience function to execute a serialized substrait plan using DuckDB.

    Unfortunately the sample query plan uses an outer join that is not supported by Acero
    when using substrait.
    """

    # use recipe for preparing the database
    duck_dbms = PrepareDatabase()

    # install and load substrait extension
    duck_dbms.LoadExtensionSubstrait()

    # then execute the substrait plan using DuckDB
    try:
        return duck_dbms.ExecuteSubstrait(plan_msg)

    except RuntimeError as exec_err:
        logger.error('DuckDB unable to execute query:')
        sys.exit(str(exec_err))


def ExecuteWithEngine(plan_msg: bytes, engine_name: str=''):
    """ A simple function to allow us to try either Acero or DuckDB. """

    if engine_name == 'DuckDB':
        return ExecuteWithDuckDB(plan_msg)

    return ExecuteWithAcero(plan_msg)


# ------------------------------
# Main Logic

if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.exit('Usage: execute-substrait.py <path-to-substrait-plan> <query-engine-name>')

    # populate the protobuf structure from binary
    default_plan_fpath = Path('resources') / 'examples' / 'average-expression.substrait'
    plan_message_fpath = Path(sys.argv[1]) or default_plan_fpath

    with open(plan_message_fpath, 'rb') as file_handle:
        logger.debug(f'Source of plan: {plan_message_fpath}')
        logger.debug(f'Using engine  : {sys.argv[2]}')

        # execute query and print results
        logger.info('Results:\n')
        logger.info(ExecuteWithEngine(file_handle.read(), sys.argv[2]))
