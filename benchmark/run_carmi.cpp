#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <iterator>
#include <random>
#include <cmath>
#include <fstream>

#include "../utils/output_files.hpp"
#include "../src/CARMI.hpp"
#include "../utils/load.hpp"
#include "../timer/rdtsc.h"

using namespace std;


void load_data(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    std::string input_file = DATA_DIR FILE_NAME;

    add_timestamp(input_file, data, MATCH_RATE ,SEED);
}

void run_carmi(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    
    vector<pair<uint64_t, uint64_t>> data_initial_sorted = data_initial;
    sort(data_initial_sorted.begin(),data_initial_sorted.end());

    CARMIMap<uint64_t,uint64_t> carmi(data_initial_sorted.begin(),data_initial_sorted.end());

    auto it = data.begin()+TIME_WINDOW;
    auto itDelete = data.begin();

    uint64_t startTime = get<1>(*(data.begin()+TIME_WINDOW));

    if(startTime < 1)
    {
        cout << "ERROR : timestamp is less than 1 " << endl;
    }
    
    uint64_t lookupCount = 0;
    uint64_t loopCounter = 0;

    uint64_t searchCycle = 0;
    uint64_t insertCycle = 0;
    uint64_t deleteCycle = 0;
    uint64_t totalCycle = 0;

    uint64_t maxTimestamp =  get<1>(data.back());
    srand(1); //When using searchTuple
    
    for(uint64_t i = startTime; i < maxTimestamp; i++ )
    {
        while (get<1>(*itDelete)  < i-TIME_WINDOW)
        {
            uint64_t tempDeleteCycles = 0;
            startTimer(&tempDeleteCycles);
            carmi.erase(get<0>(*itDelete));
            stopTimer(&tempDeleteCycles);
            deleteCycle += tempDeleteCycles;

            itDelete++;
        }

        while(get<1>(*it) == i)
        {
            
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));
            pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));

            uint64_t tempSearchCycles = 0;
            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            startTimer(&tempSearchCycles);
            carmi::carmi_range_search(carmi,searchTuple,tempJoinResult);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            carmi.insert(insertTuple);
            stopTimer(&tempInsertCycles);
            insertCycle += tempInsertCycles;

            it++;
            loopCounter++;
        }
    }

    totalCycle = searchCycle + insertCycle + deleteCycle;

    cout << "Algorithm=CARMI" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    cout << ";Fanout=" << 0 << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";SearchTime=" << (double)searchCycle/CPU_CLOCK;
    cout << ";InsertTime=" << (double)insertCycle/CPU_CLOCK;
    cout << ";DeleteTime=" << (double)deleteCycle/CPU_CLOCK;
    cout << ";TotalTime=" << (double)totalCycle/CPU_CLOCK << ";TotalCount=" << lookupCount << ";";
    cout << endl;
}

int main(int argc, char** argv)
{
    vector<tuple<uint64_t, uint64_t, uint64_t>> data;
    data.reserve(TEST_LEN);

    load_data(data);

    run_carmi(data);

    return 0;
}