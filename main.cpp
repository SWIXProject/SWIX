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

int matchRate = 1000;
int seed = 10;

int main()
{
    //load data
    vector<tuple<uint64_t, uint64_t, uint64_t>> data;
    data.reserve(TEST_LEN);

    // Random data
    std::mt19937 gen(seed);
    std::uniform_int_distribution<uint64_t> distr(1, MAX_TIMESTAMP);
    for (int i = 0; i < TEST_LEN; i++)
    {
        data.push_back(make_pair(i, distr(gen)));
    }
    sort(data.begin(),data.end(),sort_based_on_second);

    //Initialize Data
    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }

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