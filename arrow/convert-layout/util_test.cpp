// ------------------------------
// License
//
// Copyright 2024 Aldrin Montana
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


// ------------------------------
// Dependencies

#include "deps_test.hpp"


// ------------------------------
// Functions

Result<shared_ptr<Array>>
MakeTestDataForListArray() {
  auto   type_flatlist = arrow::list(arrow::int64());
  string data_listvals = R"([
                                 [1, 1, 2, 3,  4]
                                ,[2, 2, 4, 6,  8]
                                ,[3, 3, 6, 9, 12]
                            ])";

  return ArrayFromJSON(type_flatlist, data_listvals);
}


Result<shared_ptr<Array>>
MakeTestDataForNestedListArray() {
  auto   type_nestedlist = arrow::list(arrow::list(arrow::int64()));
  string data_listvals   = R"([
                                   [[1], [1, 2, 3,  4]]
                                  ,[[2], [2, 4, 6,  8]]
                                  ,[[3], [3, 6, 9, 12]]
                              ])";

  return ArrayFromJSON(type_nestedlist, data_listvals);
}

