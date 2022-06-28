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

// for templates and metaprogramming
using arrow::enable_if_primitive_ctype;

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
using arrow::Int32Builder;

// typical array types
using arrow::TypeTraits;
using arrow::StringType;
using arrow::Array;
using arrow::ArrayData;
using arrow::Int32Array;
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
using arrow::compute::ExecContext;
using arrow::compute::KernelContext;
using arrow::compute::ExecBatch;
using arrow::compute::ExecSpan;
using arrow::compute::ExecResult;
using arrow::compute::Hashing32;

// arrow functions
using arrow::MakeScalar;
using arrow::compute::FunctionRegistry;
using arrow::compute::ScalarFunction;
using arrow::compute::FunctionDoc;
using arrow::compute::InputType;
using arrow::compute::OutputType;
using arrow::compute::Arity;

// >> compute functions
using arrow::compute::Index;
using arrow::compute::IndexOptions;
using arrow::compute::default_exec_context;


// convenience functions

// >> construction
Result<shared_ptr<StringArray>>
ConstructStrArray(vector<string> src_vector);
