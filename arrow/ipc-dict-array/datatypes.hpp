#include <stdint.h>
#include <string>
#include <iostream>

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/compute/api.h>


using arrow::Array;
using arrow::DictionaryArray;
using arrow::ChunkedArray;

using ArrayPtr        = std::shared_ptr<arrow::Array>;
using ChunkedArrayPtr = std::shared_ptr<arrow::ChunkedArray>;
using TablePtr        = std::shared_ptr<arrow::Table>;
