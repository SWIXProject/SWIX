#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <iterator>
#include <random>
#include <cmath>
#include <fstream>

#include "src/Swix.hpp"
#include "src/SwixTuner.hpp"
#include "utils/load.hpp"

using namespace std;

// #define RUN_SOSD

//Place Holders
#ifdef RUN_SOSD
string data_dir =  "/data/Documents/data/";
string data_file = "f_books";
#endif

int matchRate = 1000;
int seed = 10;

void load_data(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    string input_file = data_dir+data_file;
    add_timestamp(input_file, data, matchRate ,seed);
}

int main()
{
    //load data
    vector<tuple<uint64_t, uint64_t, uint64_t>> data;
    data.reserve(TEST_LEN);
    load_data(data);

    //Initialize Data
    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);

    //Using SOSD data
    #ifdef RUN_SOSD
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    #endif

    // Random data
    std::mt19937 gen(seed);
    std::uniform_int_distribution<uint64_t> distr(1, MAX_TIMESTAMP);
    for (int i = 0; i < TIME_WINDOW; i++)
    {
        data_initial.push_back(make_pair(i, distr(gen)));
    }
    sort(data_initial.begin(),data_initial.end(),sort_based_on_second);

    //Bulkload
    swix::SWmeta<uint64_t,uint64_t> Swix(data_initial);

    //Lookup
    pair<uint64_t,uint64_t> searchPair = make_pair(get<0>(data[int(TIME_WINDOW/2)]), get<0>(data[int(TIME_WINDOW/2)]));
    uint64_t resultCount = 0;
    Swix.lookup(searchPair,resultCount);

    //Range Search
    tuple<uint64_t,uint64_t,uint64_t> searchTuple = data[int(TIME_WINDOW/2)];
    vector<pair<uint64_t, uint64_t>> searchResult;
    Swix.range_search(searchTuple,searchResult);

    //Insert
    pair<uint64_t,uint64_t> insertPair = make_pair(get<0>(data[int(TIME_WINDOW+10)]), get<0>(data[int(TIME_WINDOW+10)]));
    Swix.insert(insertPair);

    return 0;
}