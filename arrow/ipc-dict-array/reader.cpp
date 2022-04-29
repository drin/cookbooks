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

    vector<shared_ptr<RecordBatch>> parsed_batches;
    parsed_batches.reserve(file_reader->num_record_batches());

    for (int batch_ndx = 0; batch_ndx < file_reader->num_record_batches(); ++batch_ndx) {
        auto read_result = file_reader->ReadRecordBatch(batch_ndx);
        if (not read_result.ok()) {
            std::cerr << "Failed to read record batch [" << batch_ndx << "]" << std::endl
                      << "\t" << read_result.status().message()              << std::endl
            ;

            return Status::Invalid("Unable to read table from IPC file");
        }

        parsed_batches.push_back(*read_result);
    }

    return Table::FromRecordBatches(std::move(parsed_batches));
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
