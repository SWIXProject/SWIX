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
#include "../src/Swix.hpp"
#include "../src/SwixTuner.hpp"
#include "../src/ALEX.hpp"
#include "../src/BTree.hpp"
#include "../src/PGM.hpp"
#include "../src/CARMI.hpp"
#include "../src/IMTree.hpp"
#include "../utils/load.hpp"
#include "../timer/rdtsc.h"

using namespace std;

//#define PRINT must be uncommented in parameters.hpp

#ifndef LOAD_DATA_METHOD
#define LOAD_DATA_METHOD 0
#endif

#ifndef NO_STD
#define NO_STD 1
#endif

#define PRINT_INTERVAL_PERCENTAGE 25
#define PRINT_INTERVAL PRINT_INTERVAL_PERCENTAGE*(TEST_LEN-TIME_WINDOW)/100

int size_of_map()
{
    map<uint64_t,uint64_t> m;

    return sizeof(m) + sizeof(uint64_t)*2*TIME_WINDOW;
}

void load_data(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    std::string input_file = DATA_DIR FILE_NAME;

    add_timestamp(input_file, data, MATCH_RATE ,SEED);
}

void load_data_append(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    std::string input_file = DATA_DIR FILE_NAME;

    add_timestamp_append(input_file, data, MATCH_RATE, SEED);
}

void load_data_skewness(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    std::string input_file = DATA_DIR FILE_NAME;

    add_timestamp_skewness(input_file, data, NO_STD, SEED);
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

    uint64_t searchCycle = 0;
    uint64_t insertCycle = 0;
    uint64_t totalCycle = 0;

    #ifdef TUNE
    swix::SwixTuner splitErrorTuner(INITIAL_ERROR);
    int tuneCounter = 0;
    #endif

    uint64_t maxTimestamp =  get<1>(data.back());
    srand(1); //When using searchTuple

    cout << "After Bulkload:" << endl;
    swix.print_stats();

    int printCounter = 0;
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
            searchCycle += tempSearchCycles;
            lookupCount += tempCount;
            #else
            uint64_t tempSearchCycles = 0;
            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            startTimer(&tempSearchCycles);
            swix.range_search(searchTuple,tempJoinResult);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();
            #endif

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            swix.insert(insertTuple);
            stopTimer(&tempInsertCycles);
            insertCycle += tempInsertCycles;

            it++;
            loopCounter++;

            swix::print_rootNoExponentialSearch += swix::rootNoExponentialSearch;
            swix::print_rootLengthExponentialSearch += swix::rootLengthExponentialSearch;
            swix::print_segNoExponentialSearch += swix::segNoExponentialSearch;
            swix::print_segLengthExponentialSearch += swix::segLengthExponentialSearch;
            swix::print_segNoScan += swix::segNoScan;
            swix::print_segScanNoSeg += swix::segScanNoSeg;
            swix::print_segNoRetrain += swix::segNoRetrain ;
            swix::print_segLengthMerge += swix::segLengthMerge;
            swix::print_segLengthRetrain += swix::segLengthRetrain;
            swix::print_segNoExponentialSearchAll += swix::segNoExponentialSearchAll;
            swix::print_segLengthExponentialSearchAll += swix::segLengthExponentialSearchAll;

            ++printCounter;
            if (printCounter == PRINT_INTERVAL)
            {
                cout << "Round : " << loopCounter << endl;
                swix.print_stats();
                printCounter = 0;
            }

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

    cout << "Complete:" << endl;
    swix.print_stats();
}


void run_alex(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    
    vector<pair<uint64_t, uint64_t>> data_initial_sorted = data_initial;
    sort(data_initial_sorted.begin(),data_initial_sorted.end());

    alex::Alex<uint64_t,uint64_t> alex;

    //No Bulk Load 
    for (auto it = data_initial_sorted.begin(); it != data_initial_sorted.end(); it++)
    {
        // alex.insert(*it);
        alex.insert(it->first, it->second);
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
    
    cout << "After Bulkload:" << endl;
    alex::alex_print_stats(alex);

    int printCounter = 0;
    for(uint64_t i = startTime; i < maxTimestamp; i++ )
    {
        while (get<1>(*itDelete)  < i-TIME_WINDOW)
        {
            uint64_t tempDeleteCycles = 0;
            startTimer(&tempDeleteCycles);
            alex.erase(get<0>(*itDelete));
            stopTimer(&tempDeleteCycles);
            deleteCycle += tempDeleteCycles;

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
            alex::alex_point_lookup(alex,searchPair,tempCount);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempCount;
            #else
            uint64_t tempSearchCycles = 0;
            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            startTimer(&tempSearchCycles);
            alex::alex_range_search(alex,searchTuple,tempJoinResult);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();
            #endif

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            alex.insert(insertTuple);
            stopTimer(&tempInsertCycles);
            insertCycle += tempInsertCycles;

            it++;
            loopCounter++;

            ++printCounter;
            if (printCounter == PRINT_INTERVAL)
            {
                cout << "Round : " << loopCounter << endl;
                alex::alex_print_stats(alex);
                printCounter = 0;
            }
        }
    }

    cout << "Complete:" << endl;
    alex::alex_print_stats(alex);
}

void run_pgm(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    
    vector<pair<uint64_t, uint64_t>> data_initial_sorted = data_initial;
    sort(data_initial_sorted.begin(),data_initial_sorted.end());

    pgm::DynamicPGMIndex<uint64_t,uint64_t,pgm::PGMIndex<uint64_t,FANOUT_BP>> pgm(data_initial_sorted.begin(),data_initial_sorted.end());

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
    
    cout << "After Bulkload:" << endl;
    pgm::pgm_print_stats(pgm);

    int printCounter = 0;
    for(uint64_t i = startTime; i < maxTimestamp; i++ )
    {
        while (get<1>(*itDelete)  < i-TIME_WINDOW)
        {
            uint64_t tempDeleteCycles = 0;
            startTimer(&tempDeleteCycles);
            pgm.erase(get<0>(*itDelete));
            stopTimer(&tempDeleteCycles);
            deleteCycle += tempDeleteCycles;

            itDelete++;
        }

        while(get<1>(*it) == i)
        {
            
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));

            #if MATCH_RATE == 1
            pair<uint64_t,uint64_t> searchPair = make_pair(get<0>(searchTuple), get<1>(searchTuple));
            uint64_t tempCount = 0;
            uint64_t tempSearchCycles = 0;
            startTimer(&tempSearchCycles);
            pgm::pgm_point_lookup(pgm,searchPair,tempCount);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempCount;
            #else
            uint64_t tempSearchCycles = 0;
            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            startTimer(&tempSearchCycles);
            pgm::pgm_range_search(pgm,searchTuple,tempJoinResult);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();
            #endif

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            pgm.insert_or_assign(get<0>(*it),get<1>(*it));
            stopTimer(&tempInsertCycles);
            insertCycle += tempInsertCycles;

            it++;
            loopCounter++;

            ++printCounter;
            if (printCounter == PRINT_INTERVAL)
            {
                cout << "Round : " << loopCounter << endl;
                pgm::pgm_print_stats(pgm);
                printCounter = 0;
            }
        }
    }

    cout << "Complete:" << endl;
    pgm::pgm_print_stats(pgm);
}

void run_btree(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    
    vector<pair<uint64_t, uint64_t>> data_initial_sorted = data_initial;
    sort(data_initial_sorted.begin(),data_initial_sorted.end());

    stx::btree_map<uint64_t,uint64_t,less<uint64_t>,btree::btree_traits_fanout<uint64_t>> btree(data_initial_sorted.begin(),data_initial_sorted.end());

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
    
    cout << "After Bulkload:" << endl;
    btree::bt_print_stats(btree);

    int printCounter = 0;
    for(uint64_t i = startTime; i < maxTimestamp; i++ )
    {
        while (get<1>(*itDelete)  < i-TIME_WINDOW)
        {
            uint64_t tempDeleteCycles = 0;
            startTimer(&tempDeleteCycles);
            btree.erase(get<0>(*itDelete));
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
            btree::bt_range_search(btree,searchTuple,tempJoinResult);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();

            #ifdef DEBUG_PRINT_OUTPUT
            print_temp_join_result(it->first, foutRet, tempJoinResult);
            #endif

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            btree.insert(insertTuple);
            stopTimer(&tempInsertCycles);
            insertCycle += tempInsertCycles;

            it++;
            loopCounter++;

            ++printCounter;
            if (printCounter == PRINT_INTERVAL)
            {
                cout << "Round : " << loopCounter << endl;
                btree::bt_print_stats(btree);
                printCounter = 0;
            }
        }
    }

    cout << "Complete:" << endl;
    btree::bt_print_stats(btree);
}

void run_imtree(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    
    imtree::IMTree<uint64_t,uint64_t> imtree(data_initial,static_cast<int>((double)TIME_WINDOW*0.125));

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

    cout << "After Bulkload:" << endl;
    imtree.print_stats();
    
    int printCounter = 0;
    for(uint64_t i = startTime; i < maxTimestamp; i++ )
    {
        while (get<1>(*itDelete)  < i-TIME_WINDOW)
        {
            itDelete++;
        }

        while(get<1>(*it) == i)
        {
            
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));
            pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));

            uint64_t tempSearchCycles = 0;
            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            startTimer(&tempSearchCycles);
            imtree.range_search(searchTuple,tempJoinResult);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            imtree.insert(insertTuple);
            stopTimer(&tempInsertCycles);
            insertCycle += tempInsertCycles;

            it++;
            loopCounter++;

            ++printCounter;
            if (printCounter == PRINT_INTERVAL)
            {
                cout << "Round : " << loopCounter << endl;
                imtree.print_stats();
                printCounter = 0;
            }
        }
    }
    cout << "Complete:" << endl;
    imtree.print_stats();
}

int main(int argc, char** argv)
{
    vector<tuple<uint64_t, uint64_t, uint64_t>> data;
    data.reserve(TEST_LEN);

    switch (LOAD_DATA_METHOD)
    {
        case 0:
            load_data(data);
            break;
        case 1:
            load_data_append(data);
            break;
        case 2:
            load_data_skewness(data);
            break;
        default:
            load_data(data);
            break;
    }


    #ifdef RUNFLAG
    switch (RUNFLAG)
    {
        case 0:
            run_swix(data);
            break;
        case 1:
            run_alex(data);
            break;
        case 2:
            run_pgm(data);
            break;
        case 3:
            run_btree(data);
            break;
        case 4:
            run_imtree(data);
            break;
    }
    #endif;

    return 0;
}