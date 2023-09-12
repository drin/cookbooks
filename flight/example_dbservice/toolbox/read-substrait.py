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

from pathlib import Path

# >> Third-party
from substrait.gen.proto.plan_pb2 import Plan


# ------------------------------
# Module variables

example_input = Path('resources') / 'examples' / 'average-expression.substrait'


if __name__ == '__main__':
    # take 1 argument that is the path to a binary file containing a serialized substrait
    # plan
    plan_message_fpath = Path(sys.argv[1]) or example_input

    # initialize a protobuf structure
    substrait_plan = Plan()

    # populate the protobuf structure from binary read from the file
    with open(plan_message_fpath, 'rb') as file_handle:
        print(f'Source of plan: {plan_message_fpath}')
        substrait_plan.ParseFromString(file_handle.read())

    # print the query plan structure
    print(substrait_plan)
