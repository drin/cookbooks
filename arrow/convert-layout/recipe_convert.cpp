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
#include "recipe_convert.hpp"



// ------------------------------
// Functions

int ValidateArgs(int argc, char **argv) {
  if (argc != 1) {
      std::cerr << "Usage: " << argv[0] << std::endl;
      return 1;
  }

  return 0;
}


int TestCaseFlatList() {
  auto result_testdata = MakeTestDataForListArray();
  if (not result_testdata.ok()) {
    std::cerr << "Failed to create test data:"              << std::endl
              << "\t" << result_testdata.status().message() << std::endl
    ;

    return 1;
  }

  auto test_data    = *result_testdata;
  auto len_testdata = std::to_string(test_data->length());

  std::cout << "Test data [" << len_testdata << "]:" << std::endl
            << test_data->ToString()                 << std::endl
  ;

  return 0;
}


int TestCaseNestedList() {
  auto result_testdata = MakeTestDataForNestedListArray();
  if (not result_testdata.ok()) {
    std::cerr << "Failed to create test data:"              << std::endl
              << "\t" << result_testdata.status().message() << std::endl
    ;

    return 1;
  }

  auto test_data    = *result_testdata;
  auto len_testdata = std::to_string(test_data->length());

  std::cout << "Test data [" << len_testdata << "]:" << std::endl
            << test_data->ToString()                 << std::endl
  ;

  return 0;
}


// ------------------------------
// Main Logic

int main(int argc, char **argv) {

  // >> Simple validation
  int status_validate { ValidateArgs(argc, argv) };
  if (status_validate) { return status_validate; }

  // >> Test cases
  int status_testcase = TestCaseFlatList();
  if (status_testcase) { return status_testcase; }

  status_testcase = TestCaseNestedList();
  if (status_testcase) { return status_testcase; }

  return 0;
}
