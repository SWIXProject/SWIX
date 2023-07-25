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

void run_vector(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    
    auto it = data.begin()+TIME_WINDOW;
    auto itDelete = data.begin();

    uint64_t startTime = get<1>(*(data.begin()+TIME_WINDOW));

    if(startTime < 1)
    {
        cout << "ERROR : timestamp is less than 1 " << endl;
    }

    uint64_t initialSize = sizeof(vector<pair<uint64_t, uint64_t>>) + sizeof(pair<uint64_t, uint64_t>) * data_initial.size();
    
    uint64_t loopCounter = 0;
    uint64_t totalSize = 0;

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
            it++;
            totalSize += sizeof(vector<pair<uint64_t, uint64_t>>) + sizeof(pair<uint64_t, uint64_t>) * (it - itDelete);

            loopCounter++;
        }
    }

    uint64_t finalSize = sizeof(vector<pair<uint64_t, uint64_t>>) + sizeof(pair<uint64_t, uint64_t>) * (it - itDelete);
    
    cout << "Data=" << FILE_NAME << ";InitialSize=" << initialSize << ";AvgSize=" << (double)totalSize/loopCounter << ";FinalSize=" << finalSize << endl;
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

    uint64_t initialSize = swix.get_total_size_in_bytes();
    
    uint64_t lookupCount = 0;
    uint64_t loopCounter = 0;

    uint64_t totalSize = 0;

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

            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            swix.range_search(searchTuple,tempJoinResult);
            lookupCount += tempJoinResult.size();

            swix.insert(insertTuple);

            totalSize += swix.get_total_size_in_bytes();

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

    uint64_t finalSize = swix.get_total_size_in_bytes();

    #ifdef TUNE
    cout << "Algorithm=SWIXTune";
    #else
    cout << "Algorithm=SWIX";
    #endif
    cout << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    cout << ";Fanout=" << 0 << ";SplitError=" << INITIAL_ERROR << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";InitialSize=" << initialSize << ";InitialSizeExternal=" << initialSize;
    cout << ";AvgSize=" << (double)totalSize/loopCounter << ";AvgSizeExternal=" << (double)totalSize/loopCounter;
    cout << ";FinalSize=" << finalSize << ";FinalSizeExternal=" << finalSize << ";TotalCount=" << lookupCount << ";";
    cout << endl;
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

    //No Bulk Load (Sorted)
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
    
    uint64_t initialSize = alex::alex_get_total_size_in_bytes(alex);
    uint64_t initialSizeExternal = alex::alex_get_total_size_in_bytes(alex) + size_of_map();

    uint64_t lookupCount = 0;
    uint64_t loopCounter = 0;

    uint64_t totalSize = 0;
    uint64_t totalSizeExternal = 0;

    uint64_t maxTimestamp =  get<1>(data.back());
    srand(1); //When using searchTuple
    
    for(uint64_t i = startTime; i < maxTimestamp; i++ )
    {
        while (get<1>(*itDelete)  < i-TIME_WINDOW)
        {
            alex.erase(get<0>(*itDelete));

            itDelete++;
        }

        while(get<1>(*it) == i)
        {
            
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));
            pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));

            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            alex::alex_range_search(alex,searchTuple,tempJoinResult);
            lookupCount += tempJoinResult.size();

            alex.insert(insertTuple);

            totalSize += alex::alex_get_total_size_in_bytes(alex);
            totalSizeExternal += alex::alex_get_total_size_in_bytes(alex) + size_of_map();

            it++;
            loopCounter++;
        }
    }

    uint64_t finalSize = alex::alex_get_total_size_in_bytes(alex);
    uint64_t finalSizeExternal = alex::alex_get_total_size_in_bytes(alex) + size_of_map();

    cout << "Algorithm=Alex" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    cout << ";Fanout=" << 0 << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";InitialSize=" << initialSize << ";InitialSizeExternal=" << initialSizeExternal;
    cout << ";AvgSize=" << (double)totalSize/loopCounter << ";AvgSizeExternal=" << (double)totalSizeExternal/loopCounter;
    cout << ";FinalSize=" << finalSize << ";FinalSizeExternal=" << finalSizeExternal << ";TotalCount=" << lookupCount << ";";
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
    
    uint64_t initialSize = pgm::pgm_get_total_size_in_bytes(pgm);
    uint64_t initialSizeExternal = pgm::pgm_get_total_size_in_bytes(pgm) + size_of_map();

    uint64_t lookupCount = 0;
    uint64_t loopCounter = 0;

    uint64_t totalSize = 0;
    uint64_t totalSizeExternal = 0;

    uint64_t maxTimestamp =  get<1>(data.back());
    srand(1); //When using searchTuple
    
    for(uint64_t i = startTime; i < maxTimestamp; i++ )
    {
        while (get<1>(*itDelete)  < i-TIME_WINDOW)
        {
            pgm.erase(get<0>(*itDelete));

            itDelete++;
        }

        while(get<1>(*it) == i)
        {
            
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));

            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            pgm::pgm_range_search(pgm,searchTuple,tempJoinResult);
            lookupCount += tempJoinResult.size();

            pgm.insert_or_assign(get<0>(*it),get<1>(*it));

            totalSize += pgm::pgm_get_total_size_in_bytes(pgm);
            totalSizeExternal += pgm::pgm_get_total_size_in_bytes(pgm) + size_of_map();

            it++;
            loopCounter++;
        }
    }

    uint64_t finalSize = pgm::pgm_get_total_size_in_bytes(pgm);
    uint64_t finalSizeExternal = pgm::pgm_get_total_size_in_bytes(pgm) + size_of_map();

    cout << "Algorithm=PGM" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    cout << ";Fanout=" << FANOUT_BP << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";InitialSize=" << initialSize << ";InitialSizeExternal=" << initialSizeExternal;
    cout << ";AvgSize=" << (double)totalSize/loopCounter << ";AvgSizeExternal=" << (double)totalSizeExternal/loopCounter;
    cout << ";FinalSize=" << finalSize << ";FinalSizeExternal=" << finalSizeExternal << ";TotalCount=" << lookupCount << ";";
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

    uint64_t initialSize = btree::bt_get_total_size_in_bytes(btree);
    uint64_t initialSizeExternal = btree::bt_get_total_size_in_bytes(btree) + size_of_map();

    auto it = data.begin()+TIME_WINDOW;
    auto itDelete = data.begin();

    uint64_t totalSize = 0;
    uint64_t totalSizeExternal = 0;

    uint64_t startTime = get<1>(*(data.begin()+TIME_WINDOW));

    if(startTime < 1)
    {
        cout << "ERROR : timestamp is less than 1 " << endl;
    }
    
    uint64_t lookupCount = 0;
    uint64_t loopCounter = 0;

    uint64_t maxTimestamp =  get<1>(data.back());
    srand(1); //When using searchTuple
    
    for(uint64_t i = startTime; i < maxTimestamp; i++ )
    {
        while (get<1>(*itDelete)  < i-TIME_WINDOW)
        {
            btree.erase(get<0>(*itDelete));
            itDelete++;
        }

        while(get<1>(*it) == i)
        {
            
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));
            pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));

            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            btree::bt_range_search(btree,searchTuple,tempJoinResult);
            lookupCount += tempJoinResult.size();

            btree.insert(insertTuple);

            totalSize += btree::bt_get_total_size_in_bytes(btree);
            totalSizeExternal += btree::bt_get_total_size_in_bytes(btree) + size_of_map();

            it++;
            loopCounter++;
        }
    }

    uint64_t finalSize = btree::bt_get_total_size_in_bytes(btree);
    uint64_t finalSizeExternal = btree::bt_get_total_size_in_bytes(btree) + size_of_map();

    cout << "Algorithm=BTree" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    cout << ";Fanout=" << FANOUT_BP << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";InitialSize=" << initialSize << ";InitialSizeExternal=" << initialSizeExternal;
    cout << ";AvgSize=" << (double)totalSize/loopCounter << ";AvgSizeExternal=" << (double)totalSizeExternal/loopCounter;
    cout << ";FinalSize=" << finalSize << ";FinalSizeExternal=" << finalSizeExternal << ";TotalCount=" << lookupCount << ";";
    cout << endl;
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
    uint64_t initialSize = imtree.get_total_size_in_bytes();
    
    uint64_t lookupCount = 0;
    uint64_t loopCounter = 0;

    uint64_t totalSize = 0;

    uint64_t maxTimestamp =  get<1>(data.back());
    srand(1); //When using searchTuple
    
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

            vector<pair<uint64_t, uint64_t>> tempJoinResult;
            imtree.range_search(searchTuple,tempJoinResult);
            lookupCount += tempJoinResult.size();

            imtree.insert(insertTuple);

            totalSize += imtree.get_total_size_in_bytes();

            it++;
            loopCounter++;
        }
    }

    uint64_t finalSize = imtree.get_total_size_in_bytes();

    cout << "Algorithm=IMTree" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    cout << ";Fanout=" << 0 << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";InitialSize=" << initialSize << ";InitialSizeExternal=" << initialSize;
    cout << ";AvgSize=" << (double)totalSize/loopCounter << ";AvgSizeExternal=" << (double)totalSize/loopCounter;
    cout << ";FinalSize=" << finalSize << ";FinalSizeExternal=" << finalSize << ";TotalCount=" << lookupCount << ";";
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
            run_swix(data);
            break;
        case 1:
            run_alex(data);
            break;
        case 2:
            run_pgm(data);
            run_btree(data);
            break;
        case 3:
            run_imtree(data);
            break;
        case 4:
            run_pgm(data);
            break;
        case 5:
            run_btree(data);
            break;
        case 6:
            run_vector(data);
            break;
    }
    #else
    run_swix(data);
    run_alex(data);
    run_pgm(data);
    run_btree(data);
    run_imtree(data);
    #endif;

    return 0;
}