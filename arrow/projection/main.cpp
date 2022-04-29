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
    if (argc != 2) {
        std::cerr << "Usage: read-test <path-to-input-directory>" << std::endl;
        return 1;
    }

    // read the test data from a file in IPC format
    auto test_filepath  = ConstructFileUri(argv[1]);
    auto dataset_result = DatasetFromFile(test_filepath);
    if (not dataset_result.ok()) {
        std::cerr << "Failed to read table from IPC file:"   << std::endl
                  << "\t" << dataset_result.status().message() << std::endl
        ;

        return 1;
    }

    // [DEBUG] print the table for visibility
    auto table_result = ProjectFromDataset(*dataset_result, {}, nullptr);
    if (not table_result.ok()) {
        std::cerr << "Failed to project from dataset" << std::endl;
        return 1;
    }

    // std::cout << (*table_result)->ToString() << std::endl;
    PrintTable(*table_result, 0, 10);
    std::cout << "Result columns: " << (*table_result)->num_columns() << std::endl;
    std::cout << "Result rows   : " << (*table_result)->num_rows()    << std::endl;

    return 0;
}
