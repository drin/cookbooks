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

// scalar types; these seem new as of 8.0.0
using arrow::Scalar;
using arrow::Int64Scalar;

// typical array types
using arrow::Array;
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

// recipe functions
Status
HashBatchColumns(shared_ptr<RecordBatch> source_batch, vector<int> &col_indices);

// convenience functions

// >> construction
Result<shared_ptr<StringArray>>
ConstructStrArray(vector<string> src_vector);
