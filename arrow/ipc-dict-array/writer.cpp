// ------------------------------
// Dependencies

// Local and third-party dependencies
#include "recipe.hpp"

// ------------------------------
// Macros and aliases


// ------------------------------
// Functions

Status
WriteTableToFile(string &filepath_uri, shared_ptr<Table> data_table) {
    // construct a writer object
    ARROW_ASSIGN_OR_RAISE(
         auto file_writer
        ,WriterForIPCFile(data_table->schema(), filepath_uri)
    );

    // tell the writer to write the table data
    int64_t max_chunksize = 2048;
    ARROW_RETURN_NOT_OK(file_writer->WriteTable(*data_table, max_chunksize));

    // finish the file
    return file_writer->Close();
}


int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: write-test <path-to-output-directory>" << std::endl;
        return 1;
    }

    // >> construct the test data
    auto table_result = ConstructTestTable();
    if (not table_result.ok()) {
        std::cerr << "Failed to create dictionary array:"    << std::endl
                  << "\t" << table_result.status().message() << std::endl
        ;

        return 1;
    }

    // >> write the test data to a file in IPC format
    string test_filepath { ConstructFileUri(argv[1]) };
    auto   write_status  = WriteTableToFile(test_filepath, *table_result);
    if (not write_status.ok()) {
        std::cerr << "Failed to write table to file:" << std::endl
                  << "\t" << write_status.message()   << std::endl
        ;

        return 1;
    }

    std::cout << "Constructed test table and wrote to IPC file" << std::endl;
    return 0;
}
