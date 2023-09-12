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
A module of example approaches for executing a query.
"""


# ------------------------------
# Dependencies

# >> Standard libs
import logging
import hashlib

from pathlib import Path

# >> Third-party
import pyarrow

from pyarrow import substrait

# >> Internal
from example import AddConsoleLogHandler
from example import default_loglevel


# ------------------------------
# Module variables

# >> Logging
logger = logging.getLogger(__name__)
logger.setLevel(default_loglevel)
AddConsoleLogHandler(logger)


# ------------------------------
# Functions

def HashSubstrait(plan_msg: bytes) -> str:
    return hashlib.md5(plan_msg, usedforsecurity=False).hexdigest()

def EncodeHash(hash_digest: str) -> bytes:
    return hash_digest.encode('utf-8')

def DecodeHash(encoded_hash: bytes) -> str:
    return encoded_hash.decode(encoding='utf-8')

def ExecuteSubstrait(data_provider, plan_msg: bytes) -> pyarrow.Table:
    """ Convenience function that executes substrait using Acero. """

    logger.debug('Executing substrait...')
    result_reader = substrait.run_query(plan_msg, table_provider=data_provider)

    logger.debug('Query plan executed')
    return result_reader.read_all()

