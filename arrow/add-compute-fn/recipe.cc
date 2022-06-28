// ------------------------------
// Dependencies

// Local and third-party dependencies
#include "recipe.hpp"

// ------------------------------
// Macros and aliases

//  |> documentation

/*
 * Documentation instances have 3 attributes:
 *  1. Short description
 *  2. Long  description
 *  3. Name of input arguments
 */
const FunctionDoc named_scalar_fn_doc {
   "Calculate hashes for each element of the input argument"
  ,(
     "Calculate hash values using an algorithm based on xxHash. "
     // "This returns a hash value corresponding to each row of the input."
   )
  ,{ "input_array" }
};


// ------------------------------
// Functions

/**
 * A C++ function that invokes our compute function from the function registry via
 * `CallFunction`. This allows us to register our compute function as `func_name` (e.g.
 * "named_scalar_fn") but call it as if it was typically defined (e.g.
 * `NamedScalarFn()`).
 */
ARROW_EXPORT
Result<Datum>
NamedScalarFn(const Datum &input_arg, ExecContext *ctx) {
  auto func_name = "named_scalar_fn";
  return CallFunction(func_name, { input_arg }, ctx);
}


/**
 * Kernel definitions (internal linkage).
 *
 * These are implementations of the compute function for specific data types and data
 * shapes of input arguments. For example, we have 1 kernel that takes a scalar data type
 * (single element), and we have a different kernel that takes an array data type (many
 * elements).
 */
struct NamedScalarFn {
  static Status
  Call(KernelContext *ctx, const ExecSpan &input_arg, ExecResult *out) {
    // auto size_tmpstack = 64 * std::min(input_array.length(), max_batchsize);
    ARROW_LOG(INFO) << "Calling kernel 'NamedScalarFn'";
    if (input_arg.num_values() != 1 or not input_arg[0].is_array()) {
      return Status::Invalid("Unsupported argument types or shape");
    }

    ARROW_LOG(INFO) << "Accessing input data";
    shared_ptr<Int32Array> input_arr = std::static_pointer_cast<Int32Array>(input_arg[0].array.ToArray());
    const int32_t *arr_data          = input_arr->raw_values();

    ARROW_LOG(INFO) << "Hashing inputs";
    std::hash<int32_t>   default_hash;
    std::vector<int32_t> hash_results;
    hash_results.reserve(input_arr->length());
    for (int64_t arr_ndx = 0; arr_ndx < input_arr->length(); ++arr_ndx) {
      hash_results.push_back(
        (int32_t) default_hash(arr_data[arr_ndx])
      );
    }

    ARROW_LOG(INFO) << "Moving hash results to an Array";
    Int32Builder builder;
    builder.Reserve(input_arr->length());
    builder.AppendValues(hash_results);

    ARROW_LOG(INFO) << "Finishing array";
    ARROW_ASSIGN_OR_RAISE(auto result_array, builder.Finish());

    ARROW_LOG(INFO) << "Setting kernel outputs";
    shared_ptr<ArrayData> out_arrdata = out->array_data();
    out_arrdata                       = result_array->data();

    ARROW_LOG(INFO) << "Kernel execution complete";
    return Status::OK();
  }

  static constexpr uint32_t max_batchsize = MiniBatch::kMiniBatchLength;
};


// ------------------------------
// Registration

/**
 * A convenience function that constructs a `ScalarFunction` instance named
 * "absolute_value" and registers unchecked versions of the AbsoluteValue kernel.
 */
shared_ptr<ScalarFunction>
RegisterScalarFnKernels() {
  // Instantiate a function to be registered
  auto fn_named_scalar = std::make_shared<ScalarFunction>(
     "named_scalar_fn"
    ,Arity::Unary()
    ,std::move(named_scalar_fn_doc)
  );

  // Register a kernel for each data type we want this function to accommodate
  DCHECK_OK(
    fn_named_scalar->AddKernel(
       { InputType(arrow::int32()) }
      ,OutputType(arrow::int32())
      ,NamedScalarFn::Call
    )
  );

  return fn_named_scalar;
}


/**
 * A convenience function that takes a `FunctionRegistry` pointer as an input argument.
 *
 * This function calls 2 other convenience functions, then registers the returned
 * `ScalarFunction` instance from each in the input `FunctionRegistry`.
 */
void
RegisterNamedScalarFn(FunctionRegistry *registry) {
  auto scalar_fn = RegisterScalarFnKernels();
  DCHECK_OK(registry->AddFunction(std::move(scalar_fn)));
}
