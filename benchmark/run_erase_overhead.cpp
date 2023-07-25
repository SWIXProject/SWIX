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
#include "../src/ALEX.hpp"
#include "../src/BTree.hpp"
#include "../src/PGM.hpp"
#include "../src/Vector.hpp"

#include "../utils/load.hpp"
#include "../timer/rdtsc.h"

using namespace std;

#ifndef LOAD_DATA_METHOD
#define LOAD_DATA_METHOD 0
#endif

#ifndef NO_STD
#define NO_STD 1
#endif

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
    
    for(uint64_t i = startTime; i < maxTimestamp; i++ )
    {
        while (get<1>(*itDelete)  < i-TIME_WINDOW)
        {
            ++itDelete;
        }

        while(get<1>(*it) == i)
        {
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));
            pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));

            uint64_t tempDeleteCycles = 0;
            startTimer(&tempDeleteCycles);
            alex::alex_erase(alex,insertTuple);
            stopTimer(&tempDeleteCycles);
            deleteCycle += tempDeleteCycles;

            uint64_t tempSearchCycles = 0;
            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            startTimer(&tempSearchCycles);
            alex::alex_range_search(alex,searchTuple,tempJoinResult);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            alex.insert(insertTuple);
            stopTimer(&tempInsertCycles);
            insertCycle += tempInsertCycles;

            it++;
            loopCounter++;
        }
    }

    totalCycle = searchCycle + insertCycle + deleteCycle;

    cout << "Algorithm=Alex" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    if (LOAD_DATA_METHOD == 2)
    {
        cout << ";NoStandDev=" << NO_STD;
    }
    cout << ";Fanout=" << 0 << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";SearchTime=" << (double)searchCycle/CPU_CLOCK;
    cout << ";InsertTime=" << (double)insertCycle/CPU_CLOCK;
    cout << ";DeleteTime=" << (double)deleteCycle/CPU_CLOCK;
    cout << ";TotalTime=" << (double)totalCycle/CPU_CLOCK << ";TotalCount=" << lookupCount << ";";
    cout << endl;
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
    
    for(uint64_t i = startTime; i < maxTimestamp; i++ )
    {
        while (get<1>(*itDelete)  < i-TIME_WINDOW)
        {
            ++itDelete;
        }
        
        while(get<1>(*it) == i)
        {
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));
            pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));

            uint64_t tempDeleteCycles = 0;
            startTimer(&tempDeleteCycles);
            pgm::pgm_erase(pgm,insertTuple);
            stopTimer(&tempDeleteCycles);
            deleteCycle += tempDeleteCycles;

            uint64_t tempSearchCycles = 0;
            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            startTimer(&tempSearchCycles);
            pgm::pgm_range_search(pgm,searchTuple,tempJoinResult);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            pgm.insert_or_assign(get<0>(*it),get<1>(*it));
            stopTimer(&tempInsertCycles);
            insertCycle += tempInsertCycles;

            it++;
            loopCounter++;
        }
    }

    totalCycle = searchCycle + insertCycle + deleteCycle;


    cout << "Algorithm=PGM" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    if (LOAD_DATA_METHOD == 2)
    {
        cout << ";NoStandDev=" << NO_STD;
    }
    cout << ";Fanout=" << FANOUT_BP << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";SearchTime=" << (double)searchCycle/CPU_CLOCK;
    cout << ";InsertTime=" << (double)insertCycle/CPU_CLOCK;
    cout << ";DeleteTime=" << (double)deleteCycle/CPU_CLOCK;
    cout << ";TotalTime=" << (double)totalCycle/CPU_CLOCK << ";TotalCount=" << lookupCount << ";";
    cout << endl;
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
    
    for(uint64_t i = startTime; i < maxTimestamp; i++ )
    {
        while (get<1>(*itDelete)  < i-TIME_WINDOW)
        {
            ++itDelete;
        }
        
        while(get<1>(*it) == i)
        {
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));
            pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));

            uint64_t tempDeleteCycles = 0;
            startTimer(&tempDeleteCycles);
            btree::bt_erase(btree,insertTuple);
            stopTimer(&tempDeleteCycles);
            deleteCycle += tempDeleteCycles;

            uint64_t tempSearchCycles = 0;
            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            startTimer(&tempSearchCycles);
            btree::bt_range_search(btree,searchTuple,tempJoinResult);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            btree.insert(insertTuple);
            stopTimer(&tempInsertCycles);
            insertCycle += tempInsertCycles;

            it++;
            loopCounter++;
        }
    }

    totalCycle = searchCycle + insertCycle + deleteCycle;


    cout << "Algorithm=BTree" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    if (LOAD_DATA_METHOD == 2)
    {
        cout << ";NoStandDev=" << NO_STD;
    }
    cout << ";Fanout=" << FANOUT_BP << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";SearchTime=" << (double)searchCycle/CPU_CLOCK;
    cout << ";InsertTime=" << (double)insertCycle/CPU_CLOCK;
    cout << ";DeleteTime=" << (double)deleteCycle/CPU_CLOCK;
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
            ++itDelete;
        }

        while(get<1>(*it) == i)
        {
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));
            pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));

            uint64_t tempDeleteCycles = 0;
            startTimer(&tempDeleteCycles);
            vector_erase(data_no_index,insertTuple);
            stopTimer(&tempDeleteCycles);
            deleteCycle += tempDeleteCycles;

            uint64_t tempSearchCycles = 0;
            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            startTimer(&tempSearchCycles);
            vector_sorted_range_search(data_no_index,searchTuple,tempJoinResult);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            vector_sorted_insert(data_no_index,insertTuple);
            stopTimer(&tempInsertCycles);
            insertCycle += tempInsertCycles;

            it++;
            loopCounter++;
        }
    }

    totalCycle = searchCycle + insertCycle + deleteCycle;

    cout << "Algorithm=VectorSorted" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    if (LOAD_DATA_METHOD == 2)
    {
        cout << ";NoStandDev=" << NO_STD;
    }
    cout << ";Fanout=" << 0 << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";SearchTime=" << (double)searchCycle/CPU_CLOCK;
    cout << ";InsertTime=" << (double)insertCycle/CPU_CLOCK;
    cout << ";DeleteTime=" << (double)deleteCycle/CPU_CLOCK;
    cout << ";TotalTime=" << (double)totalCycle/CPU_CLOCK << ";TotalCount=" << lookupCount << ";";
    cout << endl;
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
            ++itDelete;
        }

        while(get<1>(*it) == i)
        {
            
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));
            pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));

            uint64_t tempDeleteCycles = 0;
            startTimer(&tempDeleteCycles);
            vector_erase(data_no_index,insertTuple);
            stopTimer(&tempDeleteCycles);
            deleteCycle += tempDeleteCycles;

            uint64_t tempSearchCycles = 0;
            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            startTimer(&tempSearchCycles);
            vector_not_sorted_range_search(data_no_index,searchTuple,tempJoinResult);
            stopTimer(&tempSearchCycles);
            searchCycle += tempSearchCycles;
            lookupCount += tempJoinResult.size();

            uint64_t tempInsertCycles = 0;
            startTimer(&tempInsertCycles);
            data_no_index.push_back(insertTuple);
            stopTimer(&tempInsertCycles);
            insertCycle += tempInsertCycles;

            it++;
            loopCounter++;
        }
    }

    totalCycle = searchCycle + insertCycle + deleteCycle;

    cout << "Algorithm=VectorNotSorted" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    if (LOAD_DATA_METHOD == 2)
    {
        cout << ";NoStandDev=" << NO_STD;
    }
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
            run_alex(data);
            break;
        case 1:
            run_pgm(data);
            run_btree(data);
            break;
        case 2:
            run_pgm(data);
            break;
        case 3:
            run_btree(data);
            break;
        case 4:
            run_vector_sorted(data);
            break;
        case 5:
            run_vector_not_sorted(data);
            break;
    }
    #else
    run_alex(data);
    run_pgm(data);
    run_btree(data);
    run_vector_sorted(data);
    run_vector_not_sorted(data);
    #endif;

    return 0;
}