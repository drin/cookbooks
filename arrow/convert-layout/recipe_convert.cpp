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
// Macros and Aliases

#define ExitIfCodeNotOK(expr) {                    \
          auto return_code = expr;                 \
          if (return_code) { return return_code; } \
        }

#define ExitIfStatusNotOK(expr) {                                              \
          auto return_status = expr;                                           \
          if (not return_status.ok()) {                                        \
            std::cerr << "[Fail] |> " << return_status.message() << std::endl; \
            return 10;                                                         \
          }                                                                    \
        }

// ------------------------------
// Reference variables

constexpr int32_t bufndx_offsets = 1;


// ------------------------------
// Functions

// >> Convenience functions
int ValidateArgs(int argc, char **argv) {
  if (argc != 1) {
      std::cerr << "Usage: " << argv[0] << std::endl;
      return 1;
  }

  return 0;
}


// >> Conversion functions

// Forward declaration of a dispatch function
Status FlattenChildOfList(ArrayData& src_data, ArrayData& out_data);

/** Updates list context given ArrayData of a deep nested level (below top-level).
 *
 *  The context is expected to be a shared_ptr<int32_t*>, containing an offsets buffer to
 *  be updated by the offsets buffer in `nested_data`.
 */
Status
FlattenNestedList(ArrayData& src_data, ArrayData& out_data) {
  // A list has N offsets, plus 1 to signify if the last list is null or not
  int64_t        out_countoffs  = out_data.length + 1;
  int32_t*       out_offsetsbuf = out_data.GetMutableValues<int32_t>(bufndx_offsets);
  const int32_t* in_offsetsbuf  = src_data.GetValuesSafe<int32_t>(bufndx_offsets);

  // Resolve offsets into values buffer
  for (int64_t offset_ndx = 0; offset_ndx < out_countoffs; ++offset_ndx) {
    int32_t offset_into_child  = out_offsetsbuf[offset_ndx];
    out_offsetsbuf[offset_ndx] = in_offsetsbuf[offset_into_child];
  }

  // Update child buffer (it should eventually be a non-nested values array).
  out_data.child_data[0] = src_data.child_data[0];

  // Recurse back into function dispatch
  ArrayData& nested_data = *(src_data.child_data[0]);
  return FlattenChildOfList(nested_data, out_data);
}


/** Function dispatch based on DataType of src_data to the appropriate flatten function.
 *
 *  For example, if src_data is a list type (List, LargeList, FixedSizeList), then the
 *  structure is expected to be List<List<...>>, because the parent of src_data is also a
 *  list type (otherwise we wouldn't have called `FlattenChildOfList`).
 */
Status
FlattenChildOfList(ArrayData& src_data, ArrayData& out_data) {
  auto nested_datatype = src_data.type->id();

  // Nested types
  if (arrow::is_list(nested_datatype)) {
    return FlattenNestedList(src_data, out_data);
  }

  // Non-nested types (last iteration to finish flattening)
  else if (not arrow::is_nested(nested_datatype)) {

    // For a fixed width value array, we only update the array type
    // (value array should already be set)
    if (arrow::is_fixed_width(nested_datatype)) {
      out_data.type = arrow::list(src_data.type);
      return Status::OK();
    }
  }

  return Status::NotImplemented(
      "Unable to flatten nested type [list: "
    + src_data.type->ToString()
    + "]"
  );
}


/** Determines if the input ArrayData contains nested children.
 * 
 * In the case of a shallow, or flat, nested Array (e.g. List<Int8>), the data can be
 * "easily" accommodated by hash mechanisms in key_hash. In the case of a deep nested
 * Array (e.g. List<List<Int8>>), the inner nesting layouts need to be flattened
 * (unraveled). This is so that hash mechanisms in key_hash can correctly combine hash
 * values for a given row.
 */
bool ArrayDataIsDeepNested(ArrayData& src_data) {
  // An array cannot be deep nested without children
  if (src_data.child_data.size() <= 0) { return false; }

  // If any child is a nested array type, then the array is deep nested
  for (size_t child_ndx = 0; child_ndx < src_data.child_data.size(); ++child_ndx) {
    ArrayData& src_child = *(src_data.child_data[child_ndx]);
    if (arrow::is_nested(src_child.type->id())) { return true; }
  }

  // Otherwise, the array is shallow nested (only 1 level)
  return false;
}


/** Recurse to nested structures to flatten **/
// TODO: workflow needs to be as follows:
Status
FlattenNestedStructure(ArrayData& src_data, ArrayData& out_data) {

  // 1. Identify if structure is deep nested
  if (not ArrayDataIsDeepNested(src_data)) {
    std::cout << "Further flattening unnecessary." << std::endl;
    return Status::OK();
  }

  // 2. Identify type of top-level structure
  auto& toplevel_type = src_data.type;
  switch (toplevel_type->id()) {
    case arrow::Type::LIST: {
      // Access nested data given that top-level is a list type
      ArrayData& nested_data = *(src_data.child_data[0]);

      // 3. Recurse and flatten **nested** structure
      return FlattenChildOfList(nested_data, out_data);
    }
    
    default: {
      return Status::NotImplemented(
        "Unable to flatten data type: " + toplevel_type->ToString()
      );
    }
  }

  return Status::Invalid("Unknown error: did not expect to bypass switch-statement");
}


// >> Test cases

Status TestCaseFlatList() {
  ARROW_ASSIGN_OR_RAISE(auto test_data, MakeTestDataForListArray());

  auto len_testdata = std::to_string(test_data->length());
  std::cout << "Test data [" << len_testdata << "]:" << std::endl
            << test_data->ToString()                 << std::endl
  ;

  return Status::OK();
}


Result<shared_ptr<Buffer>>
CopyBufferFromArrayData(shared_ptr<ArrayData>& data) {
  auto default_memmgr = arrow::default_cpu_memory_manager();
  return Buffer::Copy(data->buffers[1], default_memmgr);
}


Status TestCaseNestedList() {
  std::cout << "Initializing test data..." << std::endl;
  ARROW_ASSIGN_OR_RAISE(auto test_array, MakeTestDataForNestedListArray());
  auto len_testdata = std::to_string(test_array->length());
  std::cout << "Test data ("
            << test_array->type()->ToString() << "| [" << len_testdata
            << "]):"                  << std::endl
            << test_array->ToString() << std::endl
  ;

  std::cout << "Copying ArrayData..." << std::endl;
  ArrayData& test_data = *(test_array->data());
  auto       flat_data = std::make_shared<ArrayData>(test_data);

  std::cout << "\t(original buffer size: "
            << std::to_string(flat_data->buffers[1]->size())
            << ")" << std::endl
  ;

  std::cout << "\tdeep copying offsets buffer" << std::endl;
  ARROW_ASSIGN_OR_RAISE(
     flat_data->buffers[bufndx_offsets]
    ,CopyBufferFromArrayData(flat_data)
  );

  std::cout << "\tflattening offsets..." << std::endl;
  ARROW_RETURN_NOT_OK(FlattenNestedStructure(test_data, *flat_data));

  std::cout << "\t(resulting buffer size: "
            << std::to_string(flat_data->buffers[1]->size())
            << ")" << std::endl
  ;

  std::cout << "Constructing flattened array..." << std::endl;
  auto flat_array   = arrow::MakeArray(flat_data);
  auto len_flatdata = std::to_string(flat_data->length);
  std::cout << "Flat data [" << len_flatdata << "]:" << std::endl
            << flat_array->ToString()             << std::endl
  ;

  return Status::OK();
}


// ------------------------------
// Main Logic

int CodeFromStatus(Status& arrow_status) {
  if (arrow_status.ok()) { return 0; }

  return 10;
}

int main(int argc, char **argv) {

  // CLI validation
  ExitIfCodeNotOK(ValidateArgs(argc, argv));

  // Functions to test
  ExitIfStatusNotOK(TestCaseFlatList());
  ExitIfStatusNotOK(TestCaseNestedList());

  return 0;
}
