// ------------------------------
// Dependencies

// Local and third-party dependencies
#include "example.hpp"

// ------------------------------
// Macros and aliases


// ------------------------------
// Named Functions


// >> Compute Function definition

//  |> interface

/*
 * A definition that serves as an "ergonomic" interface to a function in the function
 * registry.
 */
Result<Datum>
AbsoluteValue(const Datum& arg, ArithmeticOptions options, ExecContext* ctx) {
	auto func_name = (options.check_overflow) ?  "abs_checked" : "abs";

  return CallFunction(func_name, { arg }, ctx);
}


//  |> documentation

/*
 * Documentation instances have 3 attributes:
 *  1. Short description
 *  2. Long  description
 *  3. Name of input arguments
 */
const FunctionDoc absolute_value_doc {
   "Calculate the absolute value of the argument element-wise"
  ,(
     "Results will wrap around on integer overflow."
     "Use function 'absolute_value_checked' if you want overflow"
     "to return an error."
   )
  ,{ "x" }
};

const FunctionDoc absolute_value_checked_doc {
   "Calculate the absolute value of the argument element-wise"
  ,(
     "This function returns an error on overflow. For a variant that"
     "doesn’t fail on overflow, use function 'absolute_value_checked'."
   )
  ,{ "x" }
};



// >> Kernel definitions

/**
 * These are implementations of the compute function for specific data types and data
 * shapes of input arguments. For example, we have 1 kernel that takes a scalar data type
 * (single element), and we have a different kernel that takes an array data type (many
 * elements).
 */

/**
 * Kernels for AbsoluteValue that do not do extra checking for overflow.
 */
struct AbsoluteValue {
  /**
   * If the input type is a float (non-integer), check if it is negative.
   */
  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<T>
  Call(KernelContext*, Arg arg, Status*) {
    // if the argument is less than 0, return the argument after negation (make it
    // positive).
    return (arg < static_cast<T>(0)) ? -arg : arg;
  }

  /**
   * If the input type is a signed integer, check if it is negative.
   */
  template <typename T, typename Arg>
  static constexpr enable_if_signed_integer<T>
  Call(KernelContext*, Arg arg, Status* st) {
    return (arg < static_cast<T>(0)) ? arrow::internal::SafeSignedNegate(arg) : arg;
  }

  /**
   * If the input type is an unsigned integer, then it can't be negative.
   */
  template <typename T, typename Arg>
  static constexpr enable_if_unsigned_integer<T>
  Call(KernelContext*, Arg arg, Status*) {
      return arg;
  }
};

/**
 * Kernels for AbsoluteValue that are "safe" because they check for overflow when negating.
 */
struct AbsoluteValueChecked {

  /**
   * If the input type is a float (non-integer), assert that the argument is of the
   * expected type, then check if it is negative.
   */
  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<T>
  Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    return (arg < static_cast<T>(0)) ? -arg : arg;
  }

  /**
   * If the input type is a signed integer, assert that the argument is of the expected
   * type, then call `NegateWithOverflow`. If `NegateWithOverflow` returns true, then
   * overflow occurred and we should return a non-successful status.
   */
  template <typename T, typename Arg>
  static enable_if_signed_integer<T>
  Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");

    if (arg < static_cast<T>(0)) {
      T result = 0;

      /// `ARROW_PREDICT_FALSE` is a macro to help with branch prediction
      if (ARROW_PREDICT_FALSE(NegateWithOverflow(arg, &result))) {
        *st = Status::Invalid("overflow");
      }

      return result;
    }

    return arg;
  }

  /**
   * If the input type is an unsigned integer, assert that the argument is of the expected
   * type, then return the argument because it cannot be negative.
   */
  template <typename T, typename Arg>
  static enable_if_unsigned_integer<T>
  Call(KernelContext* ctx, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    return arg;
  }
};


// ------------------------------
// Registration

/**
 * A convenience function that constructs a `ScalarFunction` instance named
 * "absolute_value" and registers unchecked versions of the AbsoluteValue kernel.
 */
shared_ptr<ScalarFunction>
RegisterUncheckedAbsoluteValueKernels() {
  // Instantiate a function to be registered
  auto fn_absolutevalue = std::make_shared<ArithmeticFunction>(
     "absolute_value"
    ,Arity::Unary()
    ,std::move(&absolute_value_doc)
  );

  // Register a kernel for each data type we want this function to accommodate
  for (const auto& numeric_type : NumericTypes()) {
    auto exec = ArithmeticExecFromOp<ScalarUnary, AbsoluteValue>(numeric_type);
    DCHECK_OK(fn_absolutevalue->AddKernel({ numeric_type }, numeric_type, exec));
  }

  // Register a kernel that has a null output type if all input args are null.
  AddNullExec(fn_absolutevalue.get());

  return fn_absolutevalue;
}

/**
 * A convenience function that constructs a `ScalarFunction` instance named
 * "absolute_value_checked" and registers checked versions of the AbsoluteValue kernel.
 */
shared_ptr<ScalarFunction>
RegisterCheckedAbsoluteValueKernels() {

  // Instantiate a function to be registered
  auto fn_absolutevalue_checked = std::make_shared<ArithmeticFunction>(
     "absolute_value_checked"
    ,Arity::Unary()
    ,std::move(&absolute_value_checked_doc)
  );

  // Register a kernel for each data type we want this function to accommodate
  for (const auto& numeric_type : NumericTypes()) {
    auto exec = ArithmeticExecFromOp<ScalarUnaryNotNull, AbsoluteValue>(numeric_type);
    DCHECK_OK(fn_absolutevalue_checked->AddKernel({ numeric_type }, numeric_type, exec));
  }

  // Register a kernel that has a null output type if all input args are null.
  AddNullExec(fn_absolutevalue_checked.get());

  return fn_absolutevalue_checked;
}


/**
 * A convenience function that takes a `FunctionRegistry` pointer as an input argument.
 *
 * This function calls 2 other convenience functions, then registers the returned
 * `ScalarFunction` instance from each in the input `FunctionRegistry`. The called
 * convenience functions are:
 *  - `RegisterUncheckedAbsoluteValueKernels()`
 *  - `RegisterCheckedAbsoluteValueKernels()`
 */
void
RegisterAbsolueValueFunctions(FunctionRegistry *registry) {
  auto kernel_unchecked = RegisterUncheckedAbsoluteValueKernels();
	DCHECK_OK(registry->AddFunction(std::move(kernel_unchecked)));

  auto kernel_checked = RegisterCheckedAbsoluteValueKernels();
  DCHECK_OK(registry->AddFunction(std::move(kernel_checked)));
}



/*
struct ScalarUnary {
  static Status
  ExecArray(KernelContext *ctx, const ExecSpan &input_arg, ExecResult *out) {
    Status st = Status::OK();

    ArrayIterator<Int32> input_itr(input_arg[0].array);

    RETURN_NOT_OK(
      OutputAdapter<Int32>::Write(
         ctx
        ,out->array_span()
        ,[&]() -> int32_t {
           return AbsoluteValue::template Call<int32_t, int32_t>(ctx, input_itr(), &st);
         }
      )
    );

    return st;
  }
};


{
  DCHECK_OK(fn_absolutevalue->AddKernel({ numeric_type }, numeric_type, ScalarUnary::ExecArray));
}
*/
