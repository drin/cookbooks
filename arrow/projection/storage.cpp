// ------------------------------
// Dependencies

// Shared header
#include "recipe.hpp"


// ------------------------------
// Macros and aliases

using arrow::fs::FileSystemFromUri;


// ------------------------------
// Global Variables

static int MAX_BATCHES = 256;
// static int MAX_BATCHES = 1024;


// ------------------------------
// Functions

/**
 * Takes a string as a `char *` (assumes it comes from `argv`) to return a hard-coded file path as
 * a URI that matches expectations of `arrow::fs`.
 */
string
ConstructFileUri(char *file_dirpath) {
    string test_dirpath  { file_dirpath };
    string test_filepath { "file://" + test_dirpath + "/dataset.ipc" };
    std::cout << "Using local file URI:" << std::endl
              << "\t" << test_filepath   << std::endl
    ;

    return test_filepath;
}


Result<shared_ptr<RecordBatchFileReader>>
ReaderForIPCFile(const std::string &path_as_uri) {
    std::string path_to_file;

    // get a `FileSystem` instance (local fs scheme is "file://")
    ARROW_ASSIGN_OR_RAISE(auto localfs, FileSystemFromUri(path_as_uri, &path_to_file));

    // use the `FileSystem` instance to open a handle to the file
    std::cout << "Reading '" << path_to_file << "'" << std::endl;           // For debug
    ARROW_ASSIGN_OR_RAISE(auto input_file_stream, localfs->OpenInputFile(path_to_file));

    // read from the handle using `RecordBatchFileReader`
    return RecordBatchFileReader::Open(input_file_stream, IpcReadOptions::Defaults());
}


Result<shared_ptr<InMemoryDataset>>
DatasetFromFile(string &filepath_uri) {
    // construct a reader object
    ARROW_ASSIGN_OR_RAISE(auto file_reader, ReaderForIPCFile(filepath_uri));
    int batches_to_read = file_reader->num_record_batches() > MAX_BATCHES ?
          MAX_BATCHES
        : file_reader->num_record_batches()
    ;
    std::cout << "Reading "
              << batches_to_read << " of " << file_reader->num_record_batches()
              << " batches..."
              << std::endl
    ;

    vector<shared_ptr<RecordBatch>> parsed_batches;
    parsed_batches.reserve(batches_to_read);

    for (int batch_ndx = 0; batch_ndx < batches_to_read; ++batch_ndx) {
        auto read_result = file_reader->ReadRecordBatch(batch_ndx);
        if (not read_result.ok()) {
            std::cerr << "Failed to read record batch [" << batch_ndx << "]" << std::endl
                      << "\t" << read_result.status().message()              << std::endl
            ;

            return Status::Invalid("Unable to read table from IPC file");
        }

        parsed_batches.push_back(*read_result);
    }

    // Create an in-memory dataset from the parsed record batches
    return std::make_shared<InMemoryDataset>(parsed_batches[0]->schema(), parsed_batches);
}
