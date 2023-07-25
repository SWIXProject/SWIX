#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <iterator>
#include <random>
#include <numeric>
#include <cmath>
#include <fstream>

#include "../utils/output_files.hpp"
#include "../src/Vector.hpp"
#include "../utils/load.hpp"
#include "../timer/rdtsc.h"

using namespace std;

vector<uint64_t> searchCycleAll;
vector<uint64_t> insertCycleAll;
vector<uint64_t> deleteCycleAll;

void load_data(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    std::string input_file = DATA_DIR FILE_NAME;

    add_timestamp(input_file, data, MATCH_RATE ,SEED);
}

void run_vector_not_sorted(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    vector<pair<uint64_t, uint64_t>> data_no_index;
    data_no_index.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_no_index.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }

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
            data_no_index.erase(data_no_index.begin());
            stopTimer(&tempDeleteCycles);
            deleteCycle += tempDeleteCycles;
            deleteCycleAll.push_back(tempDeleteCycles);

            itDelete++;
        }

        while(get<1>(*it) == i)
        {
            
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));
            pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));

            uint64_t tempSearchCycles = 0;
            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            startTimer(&tempSearchCycles);
            vector_not_sorted_range_search(data_no_index,searchTuple,tempJoinResult);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();
            searchCycleAll.push_back(tempSearchCycles);

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            data_no_index.push_back(insertTuple);
            stopTimer(&tempInsertCycles);
            insertCycle += tempInsertCycles;
            insertCycleAll.push_back(tempInsertCycles);

            it++;
            loopCounter++;
        }
    }

    totalCycle = searchCycle + insertCycle + deleteCycle;

    sort(searchCycleAll.begin(),searchCycleAll.end());
    sort(insertCycleAll.begin(),insertCycleAll.end());
    sort(deleteCycleAll.begin(),deleteCycleAll.end());

    cout << "Algorithm=VectorNotSorted" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    cout << ";Fanout=" << 0 << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";AvgSearchLatency=" << (reduce(searchCycleAll.begin(), searchCycleAll.end(), 0.0) / searchCycleAll.size())/CPU_CLOCK;
    cout << ";LastSearchLatency=" << (double)searchCycleAll.back()/CPU_CLOCK;
    cout << ";AvgInsertLatency=" << (reduce(insertCycleAll.begin(), insertCycleAll.end(), 0.0) / insertCycleAll.size())/CPU_CLOCK;
    cout << ";LastInsertLatency=" << (double)insertCycleAll.back()/CPU_CLOCK;
    cout << ";AvgDeleteLatency=" << (reduce(deleteCycleAll.begin(), deleteCycleAll.end(), 0.0) / deleteCycleAll.size())/CPU_CLOCK;
    cout << ";LastDeleteLatency=" << (double)deleteCycleAll.back()/CPU_CLOCK;
    cout << ";TotalTime=" << (double)totalCycle/CPU_CLOCK << ";TotalCount=" << lookupCount << ";";
    cout << endl;
}

void run_vector_sorted(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }

    vector<pair<uint64_t, uint64_t>> data_no_index = data_initial;
    sort(data_no_index.begin(),data_no_index.end());

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
            vector_sorted_erase(data_no_index,get<0>(*itDelete));
            stopTimer(&tempDeleteCycles);
            deleteCycle += tempDeleteCycles;
            deleteCycleAll.push_back(tempDeleteCycles);

            itDelete++;
        }

        while(get<1>(*it) == i)
        {
            
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));
            pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));

            uint64_t tempSearchCycles = 0;
            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            startTimer(&tempSearchCycles);
            vector_sorted_range_search(data_no_index,searchTuple,tempJoinResult);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();
            searchCycleAll.push_back(tempSearchCycles);

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            vector_sorted_insert(data_no_index,insertTuple);
            stopTimer(&tempInsertCycles);
            insertCycle += tempInsertCycles;
            insertCycleAll.push_back(tempInsertCycles);

            it++;
            loopCounter++;
        }
    }

    totalCycle = searchCycle + insertCycle + deleteCycle;

    sort(searchCycleAll.begin(),searchCycleAll.end());
    sort(insertCycleAll.begin(),insertCycleAll.end());
    sort(deleteCycleAll.begin(),deleteCycleAll.end());

    cout << "Algorithm=VectorSorted" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    cout << ";Fanout=" << 0 << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";AvgSearchLatency=" << (reduce(searchCycleAll.begin(), searchCycleAll.end(), 0.0) / searchCycleAll.size())/CPU_CLOCK;
    cout << ";LastSearchLatency=" << (double)searchCycleAll.back()/CPU_CLOCK;
    cout << ";AvgInsertLatency=" << (reduce(insertCycleAll.begin(), insertCycleAll.end(), 0.0) / insertCycleAll.size())/CPU_CLOCK;
    cout << ";LastInsertLatency=" << (double)insertCycleAll.back()/CPU_CLOCK;
    cout << ";AvgDeleteLatency=" << (reduce(deleteCycleAll.begin(), deleteCycleAll.end(), 0.0) / deleteCycleAll.size())/CPU_CLOCK;
    cout << ";LastDeleteLatency=" << (double)deleteCycleAll.back()/CPU_CLOCK;
    cout << ";TotalTime=" << (double)totalCycle/CPU_CLOCK << ";TotalCount=" << lookupCount << ";";
    cout << endl;
}

int main(int argc, char** argv)
{
    vector<tuple<uint64_t, uint64_t, uint64_t>> data;
    data.reserve(TEST_LEN);
    load_data(data);
    
    #ifdef RUNFLAG
    switch (RUNFLAG)
    {
        case 0:
            run_vector_not_sorted(data);
            break;
        case 1:
            run_vector_sorted(data);
            break;
    }
    #else
    run_vector_not_sorted(data);
    run_vector_sorted(data);
    #endif;

    return 0;
}