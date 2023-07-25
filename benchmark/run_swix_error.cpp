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
    
    swix::SWmeta<uint64_t,uint64_t> swix(data_initial);

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

    vector<int> rootErrorRight;
    vector<int> rootErrorLeft;
    vector<double> segErrorRight;
    vector<double> segErrorLeft;

    rootErrorRight.push_back(swix.get_right_root_error());
    rootErrorLeft.push_back(swix.get_left_root_error());
    segErrorRight.push_back(swix.get_avg_right_seg_error());
    segErrorLeft.push_back(swix.get_avg_left_seg_error());

    #ifdef TUNE
    swix::SwixTuner splitErrorTuner(INITIAL_ERROR);
    int tuneCounter = 0;
    #endif

    uint64_t maxTimestamp =  get<1>(data.back());
    srand(1); //When using searchTuple

    int cnt = 0;
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

        cnt++;
        if (cnt == 100)
        {
            segErrorRight.push_back(swix.get_avg_right_seg_error());
            segErrorLeft.push_back(swix.get_avg_left_seg_error());
            cnt = 0;
        }

        int tempRightError = swix.get_right_root_error();
        int tempLeftError = swix.get_left_root_error();
        if (tempRightError != rootErrorRight.back() || tempLeftError != rootErrorLeft.back())
        {
            rootErrorRight.push_back(swix.get_right_root_error());
            rootErrorLeft.push_back(swix.get_left_root_error());
        }
    }

    totalCycle = externalSearchCycle + externalInsertCycle;

    cout << FILE_NAME << endl;
    cout << rootErrorRight.front();
    for (auto it = rootErrorRight.begin()+1; it != rootErrorRight.end(); it++)
    {
        cout << "," << *it;
    }
    cout << endl;
    cout << rootErrorLeft.front();
    for (auto it = rootErrorLeft.begin()+1; it != rootErrorLeft.end(); it++)
    {
        cout << "," << *it;
    }
    cout << endl;
    cout << segErrorRight.front();
    for (auto it = segErrorRight.begin()+1; it != segErrorRight.end(); it++)
    {
        cout << "," << *it;
    }
    cout << endl;
    cout << segErrorLeft.front();
    for (auto it = segErrorLeft.begin()+1; it != segErrorLeft.end(); it++)
    {
        cout << "," << *it;
    }
    cout << endl;
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