#pragma once


// ------------------------------
// Dependencies

// consolidated dependencies to keep this header concise
#include "support.hpp"

// STL
#include <functional>


// ------------------------------
// Macros and Aliases

using std::hash;


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


// ------------------------------
// Recipes for named functions


// >> Functions

/** A named function (visible in a "translation unit") that we will register. */
ARROW_EXPORT
Result<Datum>
NamedScalarFn(const Datum &input_arg, ExecContext *ctx = NULLPTR);

/** A convenience function that registers our new, named function. */
ARROW_EXPORT
void
RegisterNamedScalarFn(FunctionRegistry *registry);


// ------------------------------
// Recipes for anonymous functions
