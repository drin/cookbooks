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


// ------------------------------
// Aliases

// ----------
// Aliases for classes and structs

// >> aliases for types in standard library
using std::shared_ptr;
using std::vector;
using std::string;


// arrow util types
using arrow::Result;
using arrow::Status;
using arrow::Datum;

// arrow data types and helpers
using arrow::Array;
using arrow::ArraySpan;

using arrow::UInt32Array;
using arrow::UInt32Builder;

using arrow::Int32Array;
using arrow::Int32Builder;

using arrow::StringArray;
using arrow::StringBuilder;

// aliases for types used in `NamedScalarFn`
//    |> kernel parameters
using arrow::compute::KernelContext;
using arrow::compute::ExecSpan;
using arrow::compute::ExecResult;

//    |> common types for compute functions
using arrow::compute::FunctionRegistry;
using arrow::compute::FunctionDoc;
using arrow::compute::InputType;
using arrow::compute::OutputType;
using arrow::compute::Arity;

//    |> the "kind" of function we want
using arrow::compute::ScalarFunction;

//    |> other context types
using arrow::compute::ExecContext;
using arrow::compute::LightContext;

//    |> for hashing
using arrow::util::MiniBatch;
using arrow::util::TempVectorStack;

using arrow::compute::KeyColumnArray;
using arrow::compute::Hashing32;


// ----------
// Aliases for functions

// >> functions used for setup
using arrow::compute::default_exec_context;

// >> functions used in kernel for `NamedScalarFn`
using arrow::compute::ColumnArrayFromArrayData;


// ------------------------------
// Functions

// >> construction
Result<shared_ptr<StringArray>>
ConstructStrArray(vector<string> src_vector);
