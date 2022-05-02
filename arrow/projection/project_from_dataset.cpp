// ------------------------------
// Dependencies

// Local and third-party dependencies
#include "recipe.hpp"
#include "timing.hpp"

// ------------------------------
// Macros and aliases

// -
// Global variables

vector<string> cluster_cells = {
    "SRR3052220", "SRR3052332", "SRR3052662", "SRR3052722", "SRR3052873", "SRR3052906",
    "SRR5290080", "SRR5290081", "SRR5290082", "SRR5290083", "SRR5290084",
    "SRR5290085", "SRR5290087", "SRR5290088", "SRR5290089",
    "SRR5290090", "SRR5290091", "SRR5290092", "SRR5290093",
    "SRR5290095", "SRR5290096", "SRR5290097", "SRR5290098", "SRR5290099",
    "SRR5290101", "SRR5290102", "SRR5290103", "SRR5290104",
    "SRR5290171", "SRR5290172", "SRR5290173", "SRR5290174",
    "SRR5290176", "SRR5290177", "SRR5290178", "SRR5290179",
    "SRR5290180", "SRR5290181", "SRR5290182", "SRR5290183",
    "SRR5290184", "SRR5290186", "SRR5290187", "SRR5290188", "SRR5290189",
    "SRR5290190", "SRR5290191", "SRR5290192", "SRR5290193", "SRR5290194",
    "SRR5290195", "SRR5290196", "SRR5290197", "SRR5290198", "SRR5290199",
    "SRR5290200", "SRR5290201", "SRR5290202", "SRR5290203", "SRR5290204",
    "SRR5290205", "SRR5290206", "SRR5290207", "SRR5290208", "SRR5290209",
    "SRR5290210", "SRR5290211", "SRR5290212",
    "SRR5290285",
    "SRR5290291",
};


// ------------------------------
// Functions


int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: read-test <path-to-input-directory>" << std::endl;
        return 1;
    }

    // read the test data from a file in IPC format
    auto test_filepath  = ConstructFileUri(argv[1]);
    auto dataset_result = DatasetFromFile(test_filepath);
    if (not dataset_result.ok()) {
        std::cerr << "Failed to read table from IPC file:"   << std::endl
                  << "\t" << dataset_result.status().message() << std::endl
        ;

        return 1;
    }

    // [DEBUG] print the table for visibility
    /*
    size_t match_count  = 0;
    auto   table_filter = [&match_count](shared_ptr<Table> input_data) -> Result<shared_ptr<Array>> {
        shared_ptr<ChunkedArray> filter_col  = input_data->GetColumnByName("SRR3052220");

        BooleanBuilder filter_matches;
        filter_matches.Reserve(filter_col->length());

        for (int chunk_ndx = 0; chunk_ndx < filter_col->num_chunks(); ++chunk_ndx) {
            auto chunk_data = std::static_pointer_cast<DoubleArray>(filter_col->chunk(chunk_ndx));
            const double *chunk_vals = chunk_data->raw_values();

            for (int chunk_subndx = 0; chunk_subndx < chunk_data->length(); ++chunk_subndx) {
                if (chunk_vals[chunk_subndx] > 100) {
                    filter_matches.UnsafeAppend(true);
                    ++match_count;
                }
            }
        }

        return filter_matches.Finish();
    };
    */

    auto tstart = std::chrono::steady_clock::now();

    Expression filter_expr_sel10 = greater(field_ref(FieldRef("SRR3052220")), literal(10));
    Expression filter_expr_sel25 = or_({
         greater(field_ref(FieldRef("SRR3052220")), literal(10))
        ,greater(field_ref(FieldRef("SRR5290210")), literal(10))
        ,greater(field_ref(FieldRef("SRR5290211")), literal(10))
        ,greater(field_ref(FieldRef("SRR5290212")), literal(10))
        ,greater(field_ref(FieldRef("SRR5290285")), literal(10))
        ,greater(field_ref(FieldRef("SRR5290291")), literal(10))
    });

    auto table_result = ProjectFromDataset(*dataset_result, cluster_cells, &filter_expr_sel10);
    if (not table_result.ok()) {
        std::cerr << "Failed to project from dataset" << std::endl;
        return 1;
    }

    auto tstop = std::chrono::steady_clock::now();

    /*
    auto filtered_col = (*table_result)->GetColumnByName("SRR3052220");
    std::cout << "Filtered column: "      << filtered_col->length();
    std::cout << filtered_col->ToString() << std::endl;
    */

    // std::cout << (*table_result)->ToString() << std::endl;
    // PrintTable(*table_result, 0, 20);
    std::cout << "Result columns : " << (*table_result)->num_columns() << std::endl;
    std::cout << "Result rows    : " << (*table_result)->num_rows()    << std::endl;
    std::cout << "Start Time (ms): " << TickToMS(tstart)               << std::endl;
    std::cout << "Stop  Time (ms): " << TickToMS(tstop)                << std::endl;
    std::cout << "Duration   (ms): " << CountTicks(tstart, tstop)      << std::endl;

    return 0;
}
