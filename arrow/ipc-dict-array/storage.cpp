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


Result<shared_ptr<RecordBatchStreamReader>>
ReaderForIPCFile(const std::string &path_as_uri) {
    // For debug
    std::cout << "Reading arrow IPC-formatted file: " << path_as_uri << std::endl;
    std::string path_to_file;

    // get a `FileSystem` instance (local fs scheme is "file://")
    ARROW_ASSIGN_OR_RAISE(auto localfs, FileSystemFromUri(path_as_uri, &path_to_file));

    // use the `FileSystem` instance to open a handle to the file
    ARROW_ASSIGN_OR_RAISE(auto input_file_stream, localfs->OpenInputFile(path_to_file));

    // read from the handle using `RecordBatchStreamReader`
    return RecordBatchStreamReader::Open(input_file_stream, IpcReadOptions::Defaults());
}


Result<shared_ptr<RecordBatchWriter>>
WriterForIPCFile(shared_ptr<Schema> schema, const std::string &path_as_uri) {
    std::cout << "Writing arrow IPC-formatted file: " << path_as_uri << std::endl;
    std::string path_to_file;

    // get a `FileSystem` instance (local fs scheme is "file://")
    ARROW_ASSIGN_OR_RAISE(auto localfs, FileSystemFromUri(path_as_uri, &path_to_file));

    // create a handle for the file (expecting a RandomAccessFile type)
    ARROW_ASSIGN_OR_RAISE(auto output_file_stream, localfs->OpenOutputStream(path_to_file));

    // read from the handle using `RecordBatchStreamReader`
    return arrow::ipc::MakeFileWriter(output_file_stream, schema, IpcWriteOptions::Defaults());
}


