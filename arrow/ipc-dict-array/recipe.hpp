#pragma once

// ------------------------------
// Dependencies

// standard dependencies
#include <stdint.h>
#include <string>
#include <iostream>

// arrow dependencies
#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/dataset/api.h>
#include <arrow/compute/api.h>


// ------------------------------
// Aliases

// aliases for standard types
using std::string;
using std::shared_ptr;
using std::vector;

// arrow util types
using arrow::Result;
using arrow::Status;

// arrow data types
using arrow::DataType;
using arrow::Array;
using arrow::StringArray;
using arrow::DictionaryArray;

using arrow::ChunkedArray;
using arrow::Table;
using arrow::RecordBatch;
using arrow::Schema;

using arrow::ipc::RecordBatchWriter;
using arrow::ipc::RecordBatchFileReader;

// complex options for IPC readers and writers
using arrow::ipc::IpcReadOptions;
using arrow::ipc::IpcWriteOptions;

// arrow compute functions
using arrow::compute::DictionaryEncode;

// arrow reader/writer functions
using arrow::ipc::MakeFileWriter;


// ------------------------------
// Functions

// recipe functions (interacting with DictionaryArray)
Result<shared_ptr<StringArray>>
ConstructStrArray(vector<string> src_vector);

Result<shared_ptr<DictionaryArray>>
DictArrFromVal(vector<string> arr_vals);

Result<shared_ptr<Table>>
ConstructTestTable();


// storage functions (readers and writers)
string ConstructFileUri(char *file_dirpath);

Result<shared_ptr<RecordBatchWriter>>
WriterForIPCFile(shared_ptr<Schema> schema, const string &path_as_uri);

Result<shared_ptr<RecordBatchFileReader>>
ReaderForIPCFile(const std::string &path_as_uri);
