// ------------------------------
// Dependencies

// Shared header
#include "recipe.hpp"


// ------------------------------
// Macros and aliases

using arrow::fs::FileSystemFromUri;

// ------------------------------
// Functions

/**
 * Takes a string as a `char *` (assumes it comes from `argv`) to return a hard-coded file path as
 * a URI that matches expectations of `arrow::fs`.
 */
string
ConstructFileUri(char *file_dirpath) {
    string test_dirpath  { file_dirpath };
    string test_filepath { "file://" + test_dirpath + "/dict_array.ipc" };
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


Result<shared_ptr<RecordBatchWriter>>
WriterForIPCFile(shared_ptr<Schema> schema, const std::string &path_as_uri) {
    std::string path_to_file;

    // get a `FileSystem` instance (local fs scheme is "file://")
    ARROW_ASSIGN_OR_RAISE(auto localfs, FileSystemFromUri(path_as_uri, &path_to_file));

    // create a handle for the file (expecting a RandomAccessFile type)
    std::cout << "Writing '" << path_to_file << "'" << std::endl;               // For debug
    ARROW_ASSIGN_OR_RAISE(auto output_file_stream, localfs->OpenOutputStream(path_to_file));

    // write to the handle using `RecordBatchWriter`
    return MakeFileWriter(output_file_stream, schema, IpcWriteOptions::Defaults());
}


