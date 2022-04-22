// ------------------------------
// Dependencies

// Local and third-party dependencies
#include "recipe.hpp"

// ------------------------------
// Macros and aliases


// ------------------------------
// Functions

Result<shared_ptr<Table>>
ReadTableFromFile(string &filepath_uri) {
    // construct a reader object
    ARROW_ASSIGN_OR_RAISE(auto file_reader, ReaderForIPCFile(filepath_uri));

    // tell the reader to read all batches into a table
    auto test_table = shared_ptr<Table>(nullptr);
    ARROW_RETURN_NOT_OK(file_reader->ReadAll(&test_table));

    return test_table;
}


int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: read-test <path-to-input-directory>" << std::endl;
        return 1;
    }

    // read the test data from a file in IPC format
    auto test_filepath = ConstructFileUri(argv[1]);
    auto table_result  = ReadTableFromFile(test_filepath);
    if (not table_result.ok()) {
        std::cerr << "Failed to read table from IPC file:"   << std::endl
                  << "\t" << table_result.status().message() << std::endl
        ;

        return 1;
    }

    // [DEBUG] print the table for visibility
    std::cout << (*table_result)->ToString() << std::endl;

    return 0;
}
