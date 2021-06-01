#include <iostream>
#include <thread>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <atomic>
#include <unordered_map>
#include <cctype>
#include "queue.h"
namespace fs = std::filesystem;

static const int FileQueueMaxSizeInBytes = 256 * 1024 * 1024;
static const int OutQueueMaxSizeInBytes = 1024 * 1024 * 1024;
static const int AvgWordSizeBytes = 16;

bool process_word(std::string& word) {
    if (word.size() == 1 && std::ispunct(word[0])) return false;

    // TODO - implement here the real trimming, processing, etc ...
    return true;
}

void print (const std::unordered_map<std::string, size_t>& map) {
    for (const auto& [key, value] : map) {
        std::cout << key << " : " << value << std::endl;
    }
}

// Program Args:
// --dir "source directory" E.g -> --dir "../CountWords_Svynchuk/data"
// --mappers N "num of mapper tasks"
// --reducers M "num of reducer tasks"
// Note: Unpack the txt archive with input data located in '../CountWords_Svynchuk/data'
int main(int argc, char* argv[] )
{
    using namespace cpp_training;
    std::atomic<bool> all_data_read = false;
    std::atomic<bool> mapping_completed = false;
    size_t num_mappers = 1;
    size_t num_reducers = 1;

    std::string dir;
    if (argc > 1) {
        if (argv[1] != std::string("--dir")) {
            std::cout << "Please pass the data dir: --dir \"path/to/data\"" << std::endl;
            return 1;
        }

        dir = argv[2];
        std::cout << "Data root dir is " << dir << '\n';
        size_t num = 0;
        if (argc > 3 ) {
            if (argv[3] == std::string("--mappers")) { num = num_mappers = std::stoi(argv[4]);}
            else if (argv[3] == std::string("--reducers")) { num = num_reducers = std::stoi(argv[4]);}
            std::cout << "Number of " << argv[3] << " : " << num << '\n';
        }
        if (argc > 5 ) {
            if (argv[5] == std::string("--mappers")) {num = num_mappers = std::stoi(argv[6]);}
            else if (argv[5] == std::string("--reducers")) {num = num_reducers = std::stoi(argv[6]);}
            std::cout << "Number of " << argv[5]  << " : " <<  num << '\n';
        }
    }
    else {
        std::cout << "Please pass the data dir: --dir \"path/to/data\"" << std::endl;
        return 1;
    }
    //std::cout << "Current working path is " << fs::current_path() << '\n';

    blocking_queue<std::string> in_q {FileQueueMaxSizeInBytes};
    blocking_queue<std::unordered_map<std::string, size_t>, AvgWordSizeBytes + sizeof(size_t)> out_q {OutQueueMaxSizeInBytes};

    //////////////////////
    // Load file data task
    //////////////////////
    std::function<void(const std::string&)> load_task = [&] (const std::string& data_dir) {
        for(auto& p: fs::directory_iterator(data_dir)) {

            if (p.is_directory())
                load_task(p.path().generic_string());

            std::cout << "Buffering file: " << p.path() << '\n';
            std::ifstream file(p.path());
            //std::cout << file.rdbuf();

            std::stringstream strm;
            strm << file.rdbuf();
            in_q.put(strm.str());
        }
    };

    ////////////////////
    // Mapper task
    ////////////////////
    auto mapper_task = [&in_q, &out_q, &all_data_read] () {
        std::cout << ">> enter mapper task" << std::endl;

        while (true) {
            std::unordered_map<std::string, size_t> word_counts_map;
            auto buff = in_q.get();
            std::stringstream strm(buff);

            std::string word;
            while (strm >> word) {
                if (process_word(word))
                    word_counts_map[word]++;
            }

            if (!word_counts_map.empty())
                out_q.put(std::move(word_counts_map));

            if (all_data_read && in_q.is_empty()) break;
        }
        std::cout << "<< exiting mapper task" << std::endl;
    };

    ////////////////////
    // Reducer task
    ////////////////////
    auto reduce_task = [&out_q, &mapping_completed] () {
        std::cout << ">> enter reduce task" << std::endl;
        while (true) {
            auto container1 = out_q.get();
            if (mapping_completed && container1.empty()) {
                break;
            }
            auto container2 = out_q.get();
            if (mapping_completed && container2.empty()) {
                out_q.put(std::move(container1));
                break;
            }

            for (const auto& [key, value] : container1) {
                container2[key] += value;
            }
            out_q.put(std::move(container2));
            //std::cout << "out_q after reduce: " << out_q.size()  << std::endl;
        }
        std::cout << "<< exiting reduce task" << std::endl;
    };

    /////////////////////
    // Create Pipeline
    ////////////////////

    ///////////////////////
    // Start loading data into the input queue
    ///////////////////////
    auto start = std::chrono::system_clock::now();
    std::thread dataloader(load_task, dir);

    ///////////////////////////
    // Start all mapping tasks, each mapper
    // gets a string(file) from the input queue and process it into words count map
    ///////////////////////////
    std::vector<std::thread> mappers;
    for (size_t i=0; i<num_mappers; ++i) {
        mappers.emplace_back(mapper_task);
    }

    ///////////////////////////
    // Run reducers
    ///////////////////////////
    std::vector<std::thread> reducers;
    for (size_t i=0; i<num_reducers; ++i) {
        reducers.emplace_back(reduce_task);
    }

    ////////////////////
    // wait until all data is read and processed by mappers
    ////////////////////
    dataloader.join();
    std::cout << "Add input data has been read into app" << std::endl;
    all_data_read = true;
    in_q.unblock_readers();

    for (auto& mapper : mappers) {
        mapper.join();
    }
    std::cout << "All mapping tasks completed" << std::endl;
    mapping_completed = true;
    out_q.unblock_readers();

    ////////////////////
    // wait until all data (word maps) are reduced into a single map
    ////////////////////
    for (auto& reducer : reducers) {
        reducer.join();
    }
    std::cout << "All reducing tasks completed" << std::endl;
    std::cout << "Output queue size: " << out_q.size() << std::endl;

    auto result = out_q.get();
    auto elapsed = std::chrono::system_clock::now() - start;
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);

    std::cout << "Result contains : " << result.size() << " words count" << std::endl;
    std::cout << "Elapsed ms : " << elapsed_ms.count() << std::endl;
    //print(result);

    return 0;
}
