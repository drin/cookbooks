#pragma once


// ------------------------------
// Dependencies

#include "recipe.hpp"


// ------------------------------
// Structs and Classes

// >> Documentation for a compute function
/**
 * Create a const instance of `FunctionDoc` that contains 3 attributes:
 *  1. Short description
 *  2. Long  description (limited to 78 characters)
 *  3. Name of input arguments
 */
const FunctionDoc named_scalar_fn_doc {
   "Unary function that calculates a hash for each row of the input"
  ,"This function uses an xxHash-like algorithm which produces 32-bit hashes."
  ,{ "input_array" }
};


// >> Kernel implementations for a compute function
/**
 * Create implementations that will be associated with our compute function. When a
 * compute function is invoked, the compute API framework will delegate execution to an
 * associated kernel that matches: (1) input argument types/shapes and (2) output argument
 * types/shapes.
 *
 * Kernel implementations may be functions or may be methods (functions within a class or
 * struct).
 */
struct NamedScalarFn {

  /**
   * A kernel implementation that expects a single array as input, and outputs an array of
   * uint32 values. We write this implementation knowing what function we want to
   * associate it with ("NamedScalarFn"), but that association is made later (see
   * `RegisterScalarFnKernels()` below).
   */
  static Status
  Call(KernelContext *ctx, const ExecSpan &input_arg, ExecResult *out) {
    ARROW_LOG(INFO) << "Calling kernel 'NamedScalarFn'";
    if (input_arg.num_values() != 1 or not input_arg[0].is_array()) {
      return Status::Invalid("Unsupported argument types or shape");
    }

    // >> Initialize stack-based memory allocator with an allocator and memory size
    ARROW_LOG(INFO) << "Accessing input data";

    TempVectorStack stack_memallocator;
    auto            input_dtype_width = input_arg[0].type()->bit_width();
    if (input_dtype_width > 0) {
      ARROW_RETURN_NOT_OK(
        stack_memallocator.Init(
           ctx->exec_context()->memory_pool()
          ,input_dtype_width * max_batchsize
        )
      );
    }

    // >> Prepare input data structure for propagation to hash function
    // NOTE: "start row index" and "row count" can potentially be options in the future
    ARROW_LOG(INFO) << "Accessing input data";

    ArraySpan hash_input    = input_arg[0].array;
    int64_t   hash_startrow = 0;
    int64_t   hash_rowcount = hash_input.length;
    ARROW_ASSIGN_OR_RAISE(
       KeyColumnArray input_keycol
      ,ColumnArrayFromArrayData(hash_input.ToArrayData(), hash_startrow, hash_rowcount)
    );

    // >> Call hashing function
    ARROW_LOG(INFO) << "Calling hash function";
    vector<uint32_t> hash_results;
    hash_results.resize(hash_input.length);

    LightContext hash_ctx;
    hash_ctx.hardware_flags = ctx->exec_context()->cpu_info()->hardware_flags();
    hash_ctx.stack          = &stack_memallocator;

    Hashing32::HashMultiColumn({ input_keycol }, &hash_ctx, hash_results.data());

    // >> Prepare results of hash function for kernel output argument
    ARROW_LOG(INFO) << "Moving hash results to an Array";
    UInt32Builder builder;
    builder.Reserve(hash_results.size());
    builder.AppendValues(hash_results);

    ARROW_ASSIGN_OR_RAISE(auto result_array, builder.Finish());
    out->value = result_array->data();

    ARROW_LOG(INFO) << "Kernel execution complete";
    return Status::OK();
  }


  static constexpr uint32_t max_batchsize = MiniBatch::kMiniBatchLength;
};


// ------------------------------
// Functions


// >> Function registration and kernel association
/**
 * A convenience function that shows how we construct an instance of `ScalarFunction` that
 * will be registered in a function registry. The instance is constructed with: (1) a
 * unique name ("named_scalar_fn"), (2) an "arity" (`Arity::Unary()`), and (3) an instance
 * of `FunctionDoc`.
 *
 * The function name is used to invoke it from a function registry after it has been
 * registered. The "arity" is the cardinality of the function's parameters--1 parameter is
 * a unary function, 2 parameters is a binary function, etc. Finally, it is helpful to
 * associate the function with documentation, which uses the `FunctionDoc` struct.
 */
shared_ptr<ScalarFunction>
RegisterScalarFnKernels() {
  // Instantiate a function to be registered
  auto fn_named_scalar = std::make_shared<ScalarFunction>(
     "named_scalar_fn"
    ,Arity::Unary()
    ,std::move(named_scalar_fn_doc)
  );

  // Associate a kernel implementation with the function using
  // `ScalarFunction::AddKernel()`
  DCHECK_OK(
    fn_named_scalar->AddKernel(
       { InputType(arrow::int32()) }
      ,OutputType(arrow::uint32())
      ,NamedScalarFn::Call
    )
  );

  return fn_named_scalar;
}


/**
 * A convenience function that shows how we register a custom function with a
 * `FunctionRegistry`. To keep this simple and general, this function takes a pointer to a
 * FunctionRegistry as an input argument, then invokes `FunctionRegistry::AddFunction()`.
 */
void
RegisterNamedScalarFn(FunctionRegistry *registry) {
  auto scalar_fn = RegisterScalarFnKernels();
  DCHECK_OK(registry->AddFunction(std::move(scalar_fn)));
}


// >> Convenience functions
/**
 * An optional convenience function to easily invoke our compute function. This executes
 * our compute function by invoking `CallFunction` with the name that we used to register
 * the function ("named_scalar_fn" in this case).
 */
ARROW_EXPORT
Result<Datum>
NamedScalarFn(const Datum &input_arg, ExecContext *ctx) {
  auto func_name = "named_scalar_fn";
  return CallFunction(func_name, { input_arg }, ctx);
}

