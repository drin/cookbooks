#include "datatypes.hpp"

// TODO:
using IPCReadOpts  = arrow::ipc::IpcReadOptions ;
using IPCWriteOpts = arrow::ipc::IpcWriteOptions;

using arrow::ipc::RecordBatchStreamReader;

Result<shared_ptr<RecordBatchStreamReader>>
ReaderForIPCFile(const std::string &path_as_uri) {
    sky_debug_printf("Reading arrow IPC-formatted file: %s\n", path_as_uri.data());
    std::string path_to_file;

    // get a `FileSystem` instance (local fs scheme is "file://")
    ARROW_ASSIGN_OR_RAISE(auto localfs, arrow::fs::FileSystemFromUri(path_as_uri, &path_to_file));

    // use the `FileSystem` instance to open a handle to the file
    ARROW_ASSIGN_OR_RAISE(auto input_file_stream, localfs->OpenInputFile(path_to_file));

    // read from the handle using `RecordBatchStreamReader`
    return RecordBatchStreamReader::Open(input_file_stream, IPCReadOpts::Defaults());
}


Result<shared_ptr<RecordBatchWriter>>
WriterForIPCFile(shared_ptr<Schema> schema, const std::string &path_as_uri) {
    sky_debug_printf("Writing arrow IPC-formatted file: %s", path_as_uri.c_str());

    std::string path_to_file;

    // get a `FileSystem` instance (local fs scheme is "file://")
    ARROW_ASSIGN_OR_RAISE(auto localfs, arrow::fs::FileSystemFromUri(path_as_uri, &path_to_file));

    // create a handle for the file (expecting a RandomAccessFile type)
    ARROW_ASSIGN_OR_RAISE(auto output_file_stream, localfs->OpenOutputStream(path_to_file));

    // read from the handle using `RecordBatchStreamReader`
    return arrow::ipc::MakeFileWriter(output_file_stream, schema, IPCWriteOpts::Defaults());
}
