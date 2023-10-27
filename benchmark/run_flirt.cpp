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
#include "../src/FLIRT.hpp"
#include "../utils/load.hpp"
#include "../timer/rdtsc.h"

using namespace std;

#ifndef NO_SEGMENT
#define NO_SEGMENT TIME_WINDOW
#endif

#ifndef SNAP_SHOT
#define SNAP_SHOT TIME_WINDOW * 0.5
#endif

/*
FLIRT only runs the append workload
*/

void load_data(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    std::string input_file = DATA_DIR FILE_NAME;

    add_timestamp_append(input_file, data, MATCH_RATE, SEED);
}

void run_flirt(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    
    vector<pair<uint64_t, uint64_t>> data_initial_sorted = data_initial;
    sort(data_initial_sorted.begin(),data_initial_sorted.end());

    #ifdef TUNE
    flirt::Flirt<uint64_t> flirt(NO_SEGMENT, INITIAL_ERROR);
    #else
    flirt::Flirt<uint64_t> flirt(NO_SEGMENT);
    #endif

    for (auto & it:data_initial_sorted)
    {
        flirt.enqueue(it.first);
    }
    
    auto it = data.begin()+TIME_WINDOW;
    auto itDelete = data.begin();

    uint64_t startTime = get<1>(*(data.begin()+TIME_WINDOW));

    if(startTime < 1)
    {
        cout << "ERROR : timestamp is less than 1 " << endl;
    }
    
    uint64_t lookupCount = 0;

    uint64_t searchCycle = 0;
    uint64_t insertCycle = 0;
    uint64_t deleteCycle = 0;
    uint64_t totalCycle = 0;

    uint64_t maxTimestamp =  get<1>(data.back());
    srand(1); //When using searchTuple
    
    #ifdef OUTPUT_MAX_SEGMENT
    uint64_t maxSegment = flirt.get_n();
    #endif

    for(uint64_t i = startTime; i < maxTimestamp; i++ )
    {
        while (get<1>(*itDelete)  < i-TIME_WINDOW)
        {
            uint64_t tempDeleteCycles = 0;
            startTimer(&tempDeleteCycles);
            flirt.dequeue();
            stopTimer(&tempDeleteCycles);
            deleteCycle += tempDeleteCycles;

            itDelete++;
        }

        while(get<1>(*it) == i)
        {
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));
            pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));

            uint64_t tempSearchCycles = 0;
            vector<uint64_t> tempJoinResult;
            startTimer(&tempSearchCycles);
            flirt.range_search(get<0>(searchTuple), get<0>(searchTuple), get<2>(searchTuple), tempJoinResult);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            #ifdef TUNE
            flirt.enqueue_auto_tune(insertTuple.first);
            #else
            flirt.enqueue(insertTuple.first);
            #endif
            stopTimer(&tempInsertCycles);
            insertCycle += tempInsertCycles;

            it++;

            #ifdef OUTPUT_MAX_SEGMENT
            if (maxSegment < flirt.get_n())
            {
                maxSegment = flirt.get_n();
            }
            #endif
        }
    }

    #ifdef OUTPUT_MAX_SEGMENT
    if (maxSegment < flirt.get_n())
    {
        maxSegment = flirt.get_n();
    }
    #endif

    totalCycle = searchCycle + insertCycle + deleteCycle;

    #ifdef TUNE
    cout << "Algorithm=FLIRTAutoTune";
    #else
    cout << "Algorithm=FLIRT";
    #endif 
    cout << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    cout << ";Fanout=" << 0 << ";SplitError=" << INITIAL_ERROR << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";SearchTime=" << (double)searchCycle/CPU_CLOCK;
    cout << ";InsertTime=" << (double)insertCycle/CPU_CLOCK;
    cout << ";DeleteTime=" << (double)deleteCycle/CPU_CLOCK;
    cout << ";TotalTime=" << (double)totalCycle/CPU_CLOCK << ";TotalCount=" << lookupCount << ";";
    #ifdef OUTPUT_MAX_SEGMENT
    cout << "MaxSegments=" << maxSegment << ";";
    #endif
    cout << endl;
}


int main(int argc, char** argv)
{
    vector<tuple<uint64_t, uint64_t, uint64_t>> data;
    data.reserve(TEST_LEN);
    load_data(data);
    run_flirt(data);

    return 0;
}