#pragma once

// ------------------------------
// Dependencies

// standard dependencies
#include <stdint.h>
#include <string>
#include <iostream>

// arrow dependencies
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/exec/key_hash.h>
#include <arrow/compute/exec/util.h>
#include <arrow/compute/exec/options.h>


// ------------------------------
// Aliases

// aliases for standard types
using std::string;
using std::shared_ptr;
using std::vector;

// arrow util types
using arrow::Result;
using arrow::Status;
using arrow::Datum;

// arrow data types
using arrow::util::TempVectorStack;
using arrow::util::MiniBatch;

// scalar types; these seem new as of 8.0.0
using arrow::Scalar;
using arrow::Int64Scalar;

// typical array types
using arrow::TypeTraits;
using arrow::StringType;
using arrow::Array;
using arrow::BaseBinaryArray;
using arrow::PrimitiveArray;
using arrow::ArrayVector;
using arrow::StringArray;
using arrow::ChunkedArray;

// relational types
using arrow::Schema;
using arrow::Field;
using arrow::Table;
using arrow::RecordBatch;

// compute types
using arrow::compute::ExecBatch;
using arrow::compute::Hashing32;

// arrow functions
using arrow::MakeScalar;

// >> compute functions
using arrow::compute::Index;
using arrow::compute::IndexOptions;
using arrow::compute::default_exec_context;


// ------------------------------
// Functions

// template functions

template<typename DataType, typename ArrayType>
arrow::enable_if_base_binary<DataType, int64_t>
CalculateTempStackSize(shared_ptr<ArrayType> sample_col) {
  auto arr_slice = std::static_pointer_cast<ArrayType>(
    sample_col->Slice(0, MiniBatch::kMiniBatchLength)
  );

  return 64 * arr_slice->total_values_length();
}

template<typename DataType, typename ArrayType>
arrow::enable_if_primitive_ctype<DataType, int64_t>
CalculateTempStackSize(shared_ptr<ArrayType> sample_col) {
  return 64 * std::min(sample_col->length(), MiniBatch::kMiniBatchLength);
}


// recipe functions
Status
HashBatchColumns( shared_ptr<RecordBatch>  source_batch
                 ,vector<int>             &col_indices
                 ,int64_t                  expected_size);

// convenience functions

// >> construction
Result<shared_ptr<StringArray>>
ConstructStrArray(vector<string> src_vector);
