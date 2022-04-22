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
    // >> construct the test data
    auto table_result = ConstructTestTable();
    if (not table_result.ok()) {
        std::cerr << "Failed to create dictionary array:"    << std::endl
                  << "\t" << table_result.status().message() << std::endl
        ;

        return 1;
    }

    return 0;
}
