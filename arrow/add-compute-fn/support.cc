// ------------------------------
// Dependencies

// Local and third-party dependencies
#include "support.hpp"


// ------------------------------
// Macros and aliases


// ------------------------------
// Functions

// >> construction

/**
 * How to construct a `arrow::StringArray` from a `std::vector<std::string>`.
 */
Result<shared_ptr<StringArray>>
ConstructStrArray(vector<string> src_vector) {
    shared_ptr<StringArray> str_array;
    arrow::StringBuilder    arr_builder;

    ARROW_WARN_NOT_OK(arr_builder.Resize(src_vector.size()), "Resize");
    ARROW_WARN_NOT_OK(arr_builder.AppendValues(src_vector) , "Append");

    arrow::Status build_status = arr_builder.Finish(&str_array);
    if (not build_status.ok()) {
        std::cerr << "Failed to construct string array" << std::endl;
        return build_status;
    }

    return str_array;
}
