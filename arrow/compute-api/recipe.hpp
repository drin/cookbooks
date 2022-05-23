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

// scalar types; these seem new as of 8.0.0
using arrow::Scalar;
using arrow::Int64Scalar;

// typical array types
using arrow::Array;
using arrow::ArrayVector;
using arrow::StringArray;
using arrow::ChunkedArray;

// arrow functions
using arrow::MakeScalar;

// >> compute functions
using arrow::compute::Index;
using arrow::compute::IndexOptions;


// ------------------------------
// Functions

// recipe functions
Result<int64_t>
IndexOf(shared_ptr<ChunkedArray> source_arr, string &search_str);

// convenience functions

// >> construction
Result<shared_ptr<StringArray>>
ConstructStrArray(vector<string> src_vector);
