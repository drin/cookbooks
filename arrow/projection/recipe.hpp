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

#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/options.h>


// ------------------------------
// Aliases

// aliases for standard types
using std::string;
using std::shared_ptr;
using std::vector;

// arrow util types
using arrow::Result;
using arrow::Status;
using arrow::Datum;

// arrow data types
using arrow::DataType;
using arrow::Array;
using arrow::DoubleArray;
using arrow::StringArray;
using arrow::DictionaryArray;

using arrow::BooleanBuilder;

using arrow::ChunkedArray;
using arrow::Table;
using arrow::RecordBatch;
using arrow::Schema;
using arrow::FieldRef;

using arrow::ipc::RecordBatchWriter;
using arrow::ipc::RecordBatchFileReader;

// arrow dataset types
using arrow::dataset::InMemoryDataset;

// arrow data engine types
using arrow::compute::Expression;
using arrow::compute::ExecPlan;
using arrow::compute::ExecNode;
using arrow::compute::ExecNodeOptions;
using arrow::compute::ProjectNodeOptions;

// complex options for IPC readers and writers
using arrow::ipc::IpcReadOptions;
using arrow::ipc::IpcWriteOptions;

// arrow reader/writer functions
using arrow::ipc::MakeFileWriter;

// arrow compute functions
using arrow::compute::DictionaryEncode;
using arrow::compute::Filter;

// for arrow expressions
using arrow::compute::greater;
using arrow::compute::or_;
using arrow::compute::literal;
using arrow::compute::field_ref;

// arrow data engine functions
using arrow::compute::default_exec_factory_registry;
using arrow::compute::MakeExecNode;


// Type for a filter function is to take a table and return a Boolean Vector
using filter_type = std::function<Result<shared_ptr<Array>>(shared_ptr<Table>)>;


// ------------------------------
// Functions

// recipe functions (interacting with projections of a table)
Status
ProjectFromSource();

Status
ProjectFromIntermediate();

Result<shared_ptr<Table>>
ProjectFromTable( shared_ptr<Table>  data
                 ,vector<string>     take_attrs
                 ,filter_type       *fn_filter);

Result<shared_ptr<Table>>
ProjectFromDataset( shared_ptr<InMemoryDataset>  dataset
                   ,vector<string>               data_attrs
                   ,Expression                  *data_filter);


// storage functions (readers and writers)
string ConstructFileUri(char *file_dirpath);

Result<shared_ptr<RecordBatchFileReader>>
ReaderForIPCFile(const std::string &path_as_uri);

Result<shared_ptr<InMemoryDataset>>
DatasetFromFile(string &filepath_uri);

// convenience functions (debugging)
void PrintTable(shared_ptr<Table> table_data, int64_t offset, int64_t length);
