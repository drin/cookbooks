// ------------------------------
// Dependencies

// Local and third-party dependencies
#include "recipe.hpp"

// ------------------------------
// Macros and aliases


// ------------------------------
// Functions

/**
 * A simple main function that just constructs a single table containing a single column that is
 * backed by a `arrow::DictionaryArray`.
 */
int main(int argc, char **argv) {
    // Make some test data
    auto        str_arr_result = ConstructStrArray({ "val0", "val1", "val2", "val3", "val4" });
    ArrayVector str_arrvec     { *str_arr_result };

    auto str_chunkedarr = std::make_shared<ChunkedArray>(str_arrvec, arrow::utf8());

    // Call a convenience wrapper around `arrow::compute::Index`
    string  search_val   = "val2";
    auto    index_result = IndexOf(str_chunkedarr, search_val);
    if (not index_result.ok()) {
        std::cerr << "Could not find index for value [" << search_val << "]:" << std::endl
                  << "\t" << index_result.status().message()                  << std::endl
        ;

        return 1;
    }

    // View the result
    std::cout << "Index of value [" << search_val << "]: " << *index_result << std::endl;

    return 0;
}
