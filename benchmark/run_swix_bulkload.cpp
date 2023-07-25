#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <iterator>
#include <random>
#include <cmath>
#include <fstream>

#include "../utils/output_files.hpp"
#include "../src/Swix_Self_Analysis.hpp"
#include "../src/SwixTuner.hpp"
#include "../utils/load.hpp"
#include "../timer/rdtsc.h"

using namespace std;

#ifndef BULKLOAD
#define BULKLOAD 0
#endif

void load_data(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    std::string input_file = DATA_DIR FILE_NAME;

    add_timestamp(input_file, data, MATCH_RATE ,SEED);
}


void run_swix(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    
    uint64_t bulkLoadCycle = 0;
    startTimer(&bulkLoadCycle);
    swix::SWmeta<uint64_t,uint64_t> swix(data_initial);
    stopTimer(&bulkLoadCycle);

    auto it = data.begin()+TIME_WINDOW;
    auto itDelete = data.begin();

    uint64_t startTime = get<1>(*(data.begin()+TIME_WINDOW));

    if(startTime < 1)
    {
        cout << "ERROR : timestamp is less than 1 " << endl;
    }
    
    uint64_t lookupCount = 0;
    uint64_t loopCounter = 0;

    uint64_t externalSearchCycle = 0;
    uint64_t externalInsertCycle = 0;
    uint64_t totalCycle = 0;

    #ifdef TUNE
    swix::SwixTuner splitErrorTuner(INITIAL_ERROR);
    int tuneCounter = 0;
    #endif

    uint64_t maxTimestamp =  get<1>(data.back());
    srand(1); //When using searchTuple

    for(uint64_t i = startTime; i < maxTimestamp; i++ )
    {
        while (get<1>(*itDelete) < i-TIME_WINDOW)
        {
            itDelete++;
        }
        
        while(get<1>(*it) == i)
        {
			tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));
            pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));

            #if MATCH_RATE == 1
            pair<uint64_t,uint64_t> searchPair = make_pair(get<0>(searchTuple), get<1>(searchTuple));
            uint64_t tempCount = 0;
            uint64_t tempSearchCycles = 0;
            startTimer(&tempSearchCycles);
            swix.lookup(searchPair,tempCount);
            stopTimer(&tempSearchCycles);
            externalSearchCycle += tempSearchCycles;
            lookupCount += tempCount;
            #else
            uint64_t tempSearchCycles = 0;
            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            startTimer(&tempSearchCycles);
            swix.range_search(searchTuple,tempJoinResult);
            stopTimer(&tempSearchCycles);
            externalSearchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();
            #endif

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            swix.insert(insertTuple);
            stopTimer(&tempInsertCycles);
            externalInsertCycle += tempInsertCycles;

            it++;
            loopCounter++;

            #ifdef TUNE
            tuneCounter++;
            if (tuneCounter >= AUTO_TUNE_SIZE && swix::segNoRetrain > 25)
            {
                if (swix::tuneStage == 0)
                {                    
                    swix.set_split_error(splitErrorTuner.initial_tune());

                }
                else
                {
                    swix.set_split_error(splitErrorTuner.tune());
                }
                tuneCounter = 0;
            }
            #endif

        }
    }

    totalCycle = externalSearchCycle + externalInsertCycle;

    #ifdef TUNE
    cout << "Algorithm=SWIXTune";
    #else
    cout << "Algorithm=SWIX";
    #endif

    cout << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    cout << ";Fanout=" << 0 << ";SplitError=" << INITIAL_ERROR << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;

    #ifdef PRINT_TUNE_RATE
    cout << ";TuneRate=" << AUTO_TUNE_RATE*10;
    #endif
    #ifdef BULKLOAD
        #if (BULKLOAD == 0)
        cout << ";BulkLoadType=Derivative"; 
        #elif (BULKLOAD == 1)
        cout << ";BulkLoadType=OnePass";
        #elif (BULKLOAD == 2)
        cout << ";BulkLoadType=Equal"; 
        #endif
    #endif

    cout << ";BulkloadTime=" << (double)bulkLoadCycle/CPU_CLOCK;

    #ifdef TIME_BREAKDOWN
    cout << ";SearchTime=" << (double)searchCycle/CPU_CLOCK;
    cout << ";ScanTime=" << (double)scanCycle/CPU_CLOCK;
    cout << ";InsertTime=" << (double)insertCycle/CPU_CLOCK;
    cout << ";RetrainTime=" << (double)retrainCycle/CPU_CLOCK;;
    cout << ";TotalTime=" << (double)totalCycle/CPU_CLOCK << ";TotalCount=" << lookupCount << ";";
    #else
    cout << ";SearchTime=" << (double)externalSearchCycle/CPU_CLOCK;
    cout << ";InsertTime=" << (double)externalInsertCycle/CPU_CLOCK;
    cout << ";TotalTime=" << (double)totalCycle/CPU_CLOCK << ";TotalCount=" << lookupCount << ";";
    #endif
    cout << endl;
}

int main(int argc, char** argv)
{
    vector<tuple<uint64_t, uint64_t, uint64_t>> data;
    data.reserve(TEST_LEN);
    
    load_data(data);
    
    run_swix(data);

    return 0;
}