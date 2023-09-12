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
Interface and implementations of client-side communications with a FlightServer.
"""


# ------------------------------
# Dependencies

# >> Standard
import logging
import pdb

# >> Third-party
#   |> Arrow Flight
from pyarrow import flight

# >> Internal
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

# >> Client interface
class SampleClient:
    """
    An interface used by a client application that converts application intent into to
    queries against a mohair service. This service only knows that the server it is
    talking to can receive substrait plans.
    """

    @classmethod
    def ConnectTo(cls, service_location):
        logger.debug(f'Establishing connection to [{service_location}]')
        return cls(flight.connect(service_location))

    def __init__(self, flight_conn, **kwargs):
        super().__init__(**kwargs)

        self.__flightconn = flight_conn

    def GetFlights(self):
        """ Requests a list of flights from the remote mohair service. """

        for flight in self.__flightconn.list_flights():
            logger.debug(flight)

    def SendQueryPlan(self, plan_msg: bytes):
        """ Sends a substrait plan (as bytes) to the remote mohair service. """

        service_ctrl_conn = self.__flightconn.do_action(flight.Action('query', plan_msg))

        # stream responses until the stream is closed
        service_response = next(service_ctrl_conn)
        while True:
            # For this example, each intermediate response is a log message.
            #   - `log_msg` refers to a previous response (meaning it was an intermediate)
            #   - `service_response` refers to the most recent response
            log_msg = service_response.body

            try:
                pdb.set_trace()
                service_response = next(service_ctrl_conn)
                logger.debug(log_msg.to_pybytes().decode('utf-8'))

            except StopIteration:
                break

        # the final response received is a name for the query results; or, a name for a
        # materialized view. To get the actual results, we send a separate get request.
        # This represents an approach to separate control flow from data flow.
        mohair_ticket     = flight.Ticket(service_response.body.to_pybytes())
        service_data_conn = self.__flightconn.do_get(mohair_ticket)

        # print each received result batch (converted to pandas for nicer printing)
        for result_batch in service_data_conn:
            logger.debug(result_batch.data.to_pandas())
