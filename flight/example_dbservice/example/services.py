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
A module of example flight service interfaces.

These examples try to sketch a few interactions a computational storage service should
support and also explore the use of Substrait and a data management system like DuckDB.
Implementations are defined in another module (see `dbms.py`).

A "flight service" is a server process that accepts Arrow Flight requests.
"""


# ------------------------------
# Dependencies

# >> Standard libs
import sys
import logging
import pdb

from pathlib import Path

# >> Arrow
#   |> Core types
from pyarrow import ipc

#   |> Flight types
from pyarrow.flight import FlightDescriptor, FlightEndpoint
from pyarrow.flight import FlightServerBase, FlightInfo
from pyarrow.flight import Result, RecordBatchStream

# >> Internal libs
#   |> Modules
from example import recipes

#   |> Classes
from example.storage import LocalFS, DuckDBMS

#   |> Functions
from example.executor import HashSubstrait, EncodeHash, DecodeHash, ExecuteSubstrait

#   |> Logging
from example import AddConsoleLogHandler
from example import default_loglevel


# ------------------------------
# Module variables

# >> Logging
logger = logging.getLogger(__name__)
logger.setLevel(default_loglevel)
AddConsoleLogHandler(logger)


# ------------------------------
# Classes

# >> Storage servers that store data on computational storage devices

class MetadataService(FlightServerBase):
    """
    A facade for a service that stores data management metadata. This would be analagous
    to an "MDS" (metadata server) in Ceph, which can be queried for data placement (which
    storage server holds which objects), storage cluster information, as well as other
    metadata used by the storage system or describing data stored in the storage system.
    """

    def __init__(self, **kwargs):
        pass

    def _make_flight_info(self, dataset):
        pass

    def list_flights(self, context, criteria):
        pass

    # >> Flight Verbs
    def get_flight_info(self, context, descriptor):
        # TODO: This facade should be able to point clients to the correct storage service
        pass


class DatabaseService(FlightServerBase):
    """
    A facade for a storage service that runs on a server and is backed by a DBMS. This
    storage service receives queries and executes them against the backing DBMS.

    This is simplified for this cookbook.
    """

    @classmethod
    def ForSampleData(cls, service_location):
        duck_dbconn = recipes.PrepareDatabase()
        duck_dbconn.LoadExtensionSubstrait()

        return cls(service_location, db_conn=duck_dbconn)

    def __init__(self, service_location, db_conn,  **kwargs):
        super().__init__(service_location, **kwargs)

        self.__service_loc = service_location
        self.__db_conn     = db_conn

        self.results = {}

    def info(self):
        return (
             'DatabaseService (DuckDB)\n'
            f'\tStorage type: file-backed\n'
            f'\tService location: {self.__service_loc}'
        )

    def _make_flight_info(self, table_name):
        """
        Constructs a FlightInfo object by encoding the table name (str) to utf-8 bytes.
        Other location information (endpoints) is constructed from the service and data
        locations this DatabaseService was initialized with. Other metadata information is
        obtained from the database table (converted into an Arrow table).
        """

        logger.debug(f'Constructing FlightInfo for [{table_name}]')

        db_table = self.__db_conn.table(table_name).arrow()

        # flight objects
        db_descriptor = FlightDescriptor.for_path(table_name.encode('utf-8'))
        db_endpoints  = [
            FlightEndpoint('sample', [self.__service_loc])
        ]

        return FlightInfo(
             db_table.schema
            ,db_descriptor
            ,db_endpoints
            ,db_table.num_rows
            ,db_table.nbytes
        )

    def list_flights(self, context, criteria):
        """ A 'flight' represents a place to get many (1 or more) datasets. """

        for table_name in self.__db_conn.ShowTables():
            yield self._make_flight_info(table_name)

    # >> Flight Verbs
    def get_flight_info(self, context, descriptor):
        """
        Constructs a FlightInfo object by decoding the descriptor path from utf-8 bytes to
        a string.
        """

        # NOTE: this is hardcoded to assume only the first element in path is necessary
        return self._make_flight_info(descriptor.path[0].decode('utf-8'))

    def do_exchange(self, context, descriptor, reader, writer):
        """ Just a stub for this cookbook. """
        pass

    def do_put(self, context, descriptor, reader, writer):
        """ Just a stub for this cookbook. """
        pass

    def do_get(self, context, ticket):
        """
        Handler for a `get` request. For this, a `ticket` is expected to be the hash of
        a substrait plan, as defined by QueryPlan.__hash__().
        """

        query_id = DecodeHash(ticket.ticket)
        return RecordBatchStream(self.results[query_id])

    def action_query(self, plan_msg: bytes):
        """
        Handler for `query` action, which is called via `do_action` with 'query' as the
        `ActionType`.

        This action expects a substrait plan as bytes, and should be able to parse the
        bytes using substrait protobuf wrappers.
        """

        log_msg = 'Received query plan'
        logger.debug(log_msg)
        yield Result(log_msg.encode('utf-8'))

        query_id      = HashSubstrait(plan_msg)
        query_results = self.__db_conn.ExecuteSubstrait(plan_msg)
        log_msg       = f'Executed query plan ({query_id})'
        logger.debug(log_msg)
        yield Result(log_msg.encode('utf-8'))

        # pdb.set_trace()
        self.results[query_id] = query_results
        yield Result(EncodeHash(query_id))

    def action_unknown(self, *args, **kwargs):
        """ Handler for unknown actions that simply raises an error. """

        raise NotImplementedError

    def do_action(self, context, action):
        """
        Handler for custom-defined actions. An action consists of an `ActionType` and
        opaque bytes as a "body". The `ActionType` is used for resolving a particular
        handler (e.g. `action_query` for 'query' action type). The argument to the
        resolved handler is always the full opaque bytes.
        """

        # resolve a handler function; the default is `action_unknown`
        fn_handler = getattr(self, f'action_{action.type}', self.action_unknown)

        # return the result of the handler; which may be `NotImplementedError`
        return fn_handler(action.body.to_pybytes())


# >> Computational file server
class SmartFileService(FlightServerBase):
    """
    A facade for an intelligent shim over a simple filesystem. This service keeps track of
    "datasets" that are actively being accessed, where a dataset is logical and maps 1-1
    to a physically available file. This service should be run at a location that serves
    data, which we have designed to be a location that does not know about data at other
    locations. This is a Ceph-like approach that keeps storage servers independent.

    For this service, a "flight" is a "dataset" is a file. The file format used will
    initially be IPC stream format.
    """

    def __init__(self, root_dirpath='/tmp', **kwargs):
        super().__init__(**kwargs)
        self.root_dir = Path(root_dirpath)

        # a way to identify what datasets are in memory
        #   <file descriptor: FlightDescriptor> -> <file ticket: FlightTicket>
        self._open_files = {}

    def remove_dirtree(self, dirpath):
        if not dirpath.exists(): return

        for subpath in dirpath.iterdir():
            if subpath.is_dir(): self.remove_dirtree(subpath)
            else:                subpath.unlink(missing_ok=True)

        dirpath.rmdir()

    def init_storage(self):
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def reset_storage(self):
        self.remove_dirtree(self.root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def read_schema(self, file_handle):
        """
        Reads the schema for a flight. The schema is used when returning `FlightInfo`
        structures. For now, it seems best to keep arbitrary metadata with the schema.
        This approach means that a schema and application metadata can be accessed in a
        consistent way with a single access.
        """

        # schema = ipc.read_schema(dataset_path)
        pass

    # >> Flight Verbs
    def list_flights(self, context, criteria):
        """
        Lists "available" flights, meaning flights that are readable at this location. The
        returned `FlightInfo` has non-zero `total_records` and `total_bytes` if the
        `FlightDescriptor` is being actively accessed (some portion of it is in memory).
        """

        # Foreach active descriptor: (1) get a handle, (2) get schema and metadata
        for file_descr, file_tkt in self._open_files.items():
            file_handle = FileHandle.FromDescriptor(self.root_dir, file_descr)
            schema      = self.read_schema(file_handle)

            yield FileInfo.ForDataset(file_descr, file_tkt, schema)

    def get_flight_info(self, context, flight_descr):
        # TODO: This facade should only know how to serve data it stores
        pass

    def do_get(self, context, ticket):
        """
        Flight verb that reads data described by :ticket: and returns the results to the
        client. In the simplest case, ticket may be a file path. In the most complex case,
        ticket may be a substrait plan.
        """

        flight_file = FileHandle.FromTicket(self.root_dir, ticket)
        with flight_file.open('wb') as file_handle:
            with ipc.new_file(file_handle, reader.schema) as flight_writer:
                for data_batch in reader:
                    flight_writer.write_batch(data_batch)

    def do_put(self, context, flight_descr, reader, writer):
        """
        Flight verb that writes data (from :reader:) to the handle described by
        :flight_descr:. Note that this verb overwrites the destination handle; to update
        the handle, use `do_exchange`.
        """

        # TODO: handle the multiplicity of files
        flight_files = FileHandle.FromDescriptor(self.root_dir, flight_descr)

        with flight_files[0].open('wb') as file_handle:
            with ipc.new_file(file_handle, reader.schema) as flight_writer:
                for data_batch in reader:
                    flight_writer.write_batch(data_batch)

    def do_exchange(self, context, flight_descr, reader, writer):
        """
        Flight verb that writes data (from :reader:) to the handle described by
        :flight_descr:. Note that this verb updates the destination handle; to simply
        overwrite the handle, use `do_put`.
        """

        flight_files = FileHandle.FromDescriptor(self.root_dir, flight_descr)

        with flight_files[0].open('wb') as file_handle:
            with ipc.new_file(file_handle, reader.schema) as flight_writer:
                for data_batch in reader:
                    flight_writer.write_batch(data_batch)
