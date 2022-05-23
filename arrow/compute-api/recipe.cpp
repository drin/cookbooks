// ------------------------------
// Dependencies

// Local and third-party dependencies
#include "recipe.hpp"

// ------------------------------
// Macros and aliases


// ------------------------------
// Functions

/**
 * A recipe for calling `Index`. See:
 * https://github.com/apache/arrow/blob/apache-arrow-8.0.0/cpp/src/arrow/compute/kernels/aggregate_test.cc#L2234
 */
Result<int64_t>
IndexOf(shared_ptr<ChunkedArray> source_arr, string &search_str) {
    // Wrap the search val in `Scalar`, then wrap it in `IndexOptions`
    auto search_str_as_scalar = MakeScalar(search_str);
    IndexOptions search_opt { search_str_as_scalar };

    // Call `Index`, which takes input data and an `IndexOptions`
    // NOTE: functions in the compute API take in `arrow::Datum`s and return
    //       `arrow::Datum`, but usually the inputs are implicitly converted.
    ARROW_ASSIGN_OR_RAISE(
         arrow::Datum wrapped_result
        ,Index(source_arr, search_opt)
    );

    // This is how we explicitly get the value from the `wrapped_result`
    const Int64Scalar &result_as_scalar    = wrapped_result.scalar_as<Int64Scalar>();
    int64_t            result_as_primitive = result_as_scalar.value;

    return result_as_primitive;
}


// ------------------------------
// Convenience Functions

// >> construction

/**
 * How to construct a `arrow::StringArray` from a `std::vector<std::string>`.
 */
Result<shared_ptr<StringArray>>
ConstructStrArray(vector<string> src_vector) {
    shared_ptr<StringArray> str_array;
    arrow::StringBuilder    arr_builder;

    arr_builder.Resize(src_vector.size());
    arr_builder.AppendValues(src_vector);

    arrow::Status build_status = arr_builder.Finish(&str_array);
    if (not build_status.ok()) {
        std::cerr << "Failed to construct string array" << std::endl;
        return build_status;
    }

    return str_array;
}
