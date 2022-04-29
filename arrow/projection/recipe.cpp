// ------------------------------
// Dependencies

// Local and third-party dependencies
#include "recipe.hpp"

// ------------------------------
// Macros and aliases


// ------------------------------
// Functions

Status
ProjectFromSourceNode() {
    return Status::OK();
}


Status
ProjectFromIntermediateNode() {
    return Status::OK();
}


Result<shared_ptr<Table>>
ProjectFromTable( shared_ptr<Table>  data
                 ,vector<string>     take_attrs
                 ,filter_type       *fn_filter) {
    vector<int> take_indices;
    take_indices.reserve(take_attrs.size());

    int field_ndx = 0;
    for (string &field_name : data->ColumnNames()) {
        if (field_name == take_attrs[field_ndx]) {
            take_indices.push_back(field_ndx);
        }

        ++field_ndx;
    }

    // If no filter function provided, just do the selection
    if (fn_filter == nullptr) {
        return data->SelectColumns(take_indices);
    }

    // Otherwise, do the selection, then apply the filter
    ARROW_ASSIGN_OR_RAISE(
         shared_ptr<Table> proj_data
        ,data->SelectColumns(take_indices)
    );

    // Invoke fn_filter to get a Boolean vector (match_results)
    // Then, use arrow::compute::Filter to select from match_results
    ARROW_ASSIGN_OR_RAISE(auto match_results , (*fn_filter)(proj_data));
    ARROW_ASSIGN_OR_RAISE(auto wrapped_result, Filter(proj_data, match_results));

    return wrapped_result.table();
}


Result<shared_ptr<Table>>
ProjectFromDataset( shared_ptr<InMemoryDataset>  dataset
                   ,vector<string>               data_attrs
                   ,Expression                  *data_filter) {
    // Create a scanner to pass the expression
    ARROW_ASSIGN_OR_RAISE(auto scanbuilder, dataset->NewScan());

    // Bind the projection columns and predicate
    if (not data_attrs.empty()) {
        ARROW_RETURN_NOT_OK(scanbuilder->Project(data_attrs));
    }

    if (data_filter != nullptr) {
        ARROW_RETURN_NOT_OK(scanbuilder->Filter(*data_filter));
    }

    // Then complete the scanner and return the result
    ARROW_ASSIGN_OR_RAISE(auto batch_scanner, scanbuilder->Finish());

    return batch_scanner->ToTable();
}


// ------------------------------
// Convenience Functions


void
PrintTable(shared_ptr<Table> table_data, int64_t offset, int64_t length) {
    shared_ptr<Table> table_slice;
    int64_t  row_count = table_data->num_rows();

    std::cout << "Table Excerpt ";

    if (length > 0) {
        int64_t max_rowndx = length < row_count ? length : row_count;
        table_slice = table_data->Slice(offset, max_rowndx);
        std::cout << "(" << max_rowndx << " of " << row_count << ")";
    }

    else {
        table_slice = table_data->Slice(offset);
        std::cout << "(" << row_count << " of " << row_count << ")";
    }

    std::cout << std::endl
              << "--------------" << std::endl
              << table_slice->ToString()
              << std::endl
    ;
}


// ------------------------------
// Verbose functions to clarify the API
