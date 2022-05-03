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
    // Cluster 8
     "SRR3052220", "SRR3052332", "SRR3052662", "SRR3052722", "SRR3052873", "SRR3052906"
    ,"SRR5290080", "SRR5290081", "SRR5290082", "SRR5290083", "SRR5290084"
    ,"SRR5290085", "SRR5290087", "SRR5290088", "SRR5290089"
    ,"SRR5290090", "SRR5290091", "SRR5290092", "SRR5290093"
    ,"SRR5290095", "SRR5290096", "SRR5290097", "SRR5290098", "SRR5290099"
    ,"SRR5290101", "SRR5290102", "SRR5290103", "SRR5290104"
    ,"SRR5290171", "SRR5290172", "SRR5290173", "SRR5290174"
    ,"SRR5290176", "SRR5290177", "SRR5290178", "SRR5290179"
    ,"SRR5290180", "SRR5290181", "SRR5290182", "SRR5290183"
    ,"SRR5290184", "SRR5290186", "SRR5290187", "SRR5290188", "SRR5290189"
    ,"SRR5290190", "SRR5290191", "SRR5290192", "SRR5290193", "SRR5290194"
    ,"SRR5290195", "SRR5290196", "SRR5290197", "SRR5290198", "SRR5290199"
    ,"SRR5290200", "SRR5290201", "SRR5290202", "SRR5290203", "SRR5290204"
    ,"SRR5290205", "SRR5290206", "SRR5290207", "SRR5290208", "SRR5290209"
    ,"SRR5290210", "SRR5290211", "SRR5290212"
    ,"SRR5290285"
    ,"SRR5290291"

    // Cluster 7
    ,"SRR5289790", "SRR5289791", "SRR5289792", "SRR5289793", "SRR5289794"
    ,"SRR5289795", "SRR5289796", "SRR5289797", "SRR5289799"
    ,"SRR5289800", "SRR5289801", "SRR5289802", "SRR5289803", "SRR5289804"
    ,"SRR5289805", "SRR5289806", "SRR5289807", "SRR5289808", "SRR5289809"
    ,"SRR5289810", "SRR5289811", "SRR5289812", "SRR5289813", "SRR5289814"
    ,"SRR5289815", "SRR5289816", "SRR5289817", "SRR5289818", "SRR5289819"
    ,"SRR5289820", "SRR5289821", "SRR5289822", "SRR5289823", "SRR5289824"
    ,"SRR5289825", "SRR5289826", "SRR5289827", "SRR5289828", "SRR5289829"
    ,"SRR5289830", "SRR5289831", "SRR5289832", "SRR5289833", "SRR5289834"
    ,"SRR5289835", "SRR5289836", "SRR5289837", "SRR5289838", "SRR5289839"
    ,"SRR5289840", "SRR5289841", "SRR5289842", "SRR5289843", "SRR5289844"
    ,"SRR5289845", "SRR5289846", "SRR5289847", "SRR5289848", "SRR5289849"
    ,"SRR5289850", "SRR5289851", "SRR5289852", "SRR5289853", "SRR5289854"
    ,"SRR5289855", "SRR5289856", "SRR5289857", "SRR5289858", "SRR5289859"
    ,"SRR5289860", "SRR5289861"
    ,"SRR5290280"
    ,"SRR5290313"

    // Cluster 5
    ,"SRR3052381", "SRR3052384"
    ,"SRR3052393", "SRR3052396", "SRR3052397", "SRR3052398", "SRR3052399"
    ,"SRR3052409", "SRR3052418", "SRR3052508", "SRR3052509", "SRR3052540"
    ,"SRR3052937", "SRR3052997", "SRR3053005", "SRR3053062", "SRR5289952"
    ,"SRR5290047", "SRR5290079", "SRR5290213", "SRR5290214", "SRR5290215"
    ,"SRR5290216", "SRR5290217", "SRR5290218", "SRR5290219"
    ,"SRR5290221", "SRR5290222", "SRR5290223", "SRR5290224"
    ,"SRR5290226", "SRR5290229", "SRR5290231", "SRR5290315"
    ,"SRR5290523", "SRR5290524", "SRR5290525", "SRR5290526"
    ,"SRR5290527", "SRR5290528", "SRR5290529"
    ,"SRR5290530", "SRR5290531"
    ,"SRR5296616", "SRR5296617", "SRR5296618", "SRR5296619"
    ,"SRR5296620", "SRR5296621", "SRR5296622", "SRR5296623"
    ,"SRR5296624", "SRR5296625", "SRR5296626", "SRR5296628"
    ,"SRR5296632", "SRR5296633", "SRR5296634", "SRR5296635"
    ,"SRR5296636", "SRR5296637", "SRR5296638", "SRR5296639"
    ,"SRR5296640", "SRR5296641", "SRR5296642", "SRR5296643", "SRR5296644"
    ,"SRR5296645", "SRR5296646", "SRR5296647", "SRR5296648", "SRR5296649"
    ,"SRR5296650", "SRR5296651", "SRR5296652", "SRR5296653"
    ,"SRR5296655", "SRR5296656", "SRR5296658", "SRR5296659"
    ,"SRR5296669", "SRR5296683", "SRR5296829", "SRR5296832"
    ,"SRR5296951", "SRR5296952", "SRR5296953", "SRR5296954"
    ,"SRR5296955", "SRR5296956", "SRR5296957", "SRR5296958", "SRR5296959"
    ,"SRR5296960", "SRR5296961", "SRR5296962", "SRR5296963", "SRR5296964"
    ,"SRR5296965", "SRR5296966", "SRR5296967", "SRR5296968", "SRR5296969"
    ,"SRR5296970", "SRR5296971", "SRR5296972", "SRR5296973", "SRR5296974"
    ,"SRR5296975", "SRR5296976", "SRR5296977", "SRR5296978", "SRR5296979"
    ,"SRR5296980", "SRR5296981", "SRR5296982", "SRR5296983", "SRR5296984"
    ,"SRR5296985", "SRR5296986", "SRR5296987", "SRR5296988", "SRR5296989"
    ,"SRR5296990", "SRR5296991", "SRR5296993", "SRR5296994"
    ,"SRR5296995", "SRR5296996", "SRR5296997", "SRR5296999"
    ,"SRR5297000", "SRR5297036", "SRR5297041", "SRR5297064", "SRR5456189"
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

    auto table_result = ProjectFromDataset(*dataset_result, cluster_cells, &filter_expr_sel25);
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
