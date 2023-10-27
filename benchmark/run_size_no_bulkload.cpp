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

#ifndef LOAD_DATA_METHOD
#define LOAD_DATA_METHOD 0
#endif

#ifndef SNAP_SHOT
#define SNAP_SHOT TIME_WINDOW * 0.5
#endif

#ifndef NO_STD
#define NO_STD 1
#endif

#ifdef OUTPUT_X_AXIS
vector<uint64_t> noUpdate;
uint64_t accumulate_cnt = 0;
#endif

vector<uint64_t> indexSize;

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

void run_vector(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    int cnt = 0;
    auto it = data.begin();
    auto itDelete = data.begin();
    while (it != data.begin()+TIME_WINDOW)
    {
        uint64_t time = get<1>(*it);
        uint64_t lowerLimit = ((double)time - TIME_WINDOW < 0)? 0 : time-TIME_WINDOW; 

        while (get<1>(*itDelete) < lowerLimit)
        {
            itDelete++;
        }
        
        #ifdef OUTPUT_X_AXIS
        ++accumulate_cnt;
        #endif

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            indexSize.push_back(sizeof(vector<pair<uint64_t, uint64_t>>) + sizeof(pair<uint64_t, uint64_t>) * (it - itDelete));
            cnt = 0;

            #ifdef OUTPUT_X_AXIS
            noUpdate.push_back(accumulate_cnt);
            #endif
        }
        ++it;
    }

    uint64_t initialSize = sizeof(vector<pair<uint64_t, uint64_t>>) + sizeof(pair<uint64_t, uint64_t>) * (it - itDelete);
    indexSize.push_back(sizeof(vector<pair<uint64_t, uint64_t>>) + sizeof(pair<uint64_t, uint64_t>) * (it - itDelete));
    cnt = 0;
    #ifdef OUTPUT_X_AXIS
    noUpdate.push_back(accumulate_cnt);
    #endif

    it = data.begin()+TIME_WINDOW;
    itDelete = data.begin();

    uint64_t startTime = get<1>(*(data.begin()+TIME_WINDOW));

    if(startTime < 1)
    {
        cout << "ERROR : timestamp is less than 1 " << endl;
    }

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
        }

        #ifdef OUTPUT_X_AXIS
        ++accumulate_cnt;
        #endif

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            indexSize.push_back(sizeof(vector<pair<uint64_t, uint64_t>>) + sizeof(pair<uint64_t, uint64_t>) * (it - itDelete));
            #ifdef OUTPUT_X_AXIS
            noUpdate.push_back(accumulate_cnt);
            #endif
            cnt = 0;
        }
    }

    uint64_t finalSize = sizeof(vector<pair<uint64_t, uint64_t>>) + sizeof(pair<uint64_t, uint64_t>) * (it - itDelete);
    indexSize.push_back(sizeof(vector<pair<uint64_t, uint64_t>>) + sizeof(pair<uint64_t, uint64_t>) * (it - itDelete));
    #ifdef OUTPUT_X_AXIS
    noUpdate.push_back(accumulate_cnt);
    #endif
    
    cout << "Algorithm=Vector";
    cout << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    if (LOAD_DATA_METHOD == 2)
    {
        cout << ";NoStandDev=" << NO_STD;
    }
    cout << ";Fanout=" << 0 << ";SplitError=" << INITIAL_ERROR << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";MapSize=0" << ";InitialSize=" << initialSize << ";FinalSize=" << finalSize;
    #ifdef OUTPUT_X_AXIS
    cout << ";X=[" << noUpdate.front();
    for (auto it = noUpdate.begin()+1; it != noUpdate.end(); ++it)
    {
        cout << "," << *it;
    }
    cout << "]";
    #endif
    cout << ";Size=[" << indexSize.front();
    for (auto it = indexSize.begin()+1; it != indexSize.end(); ++it)
    {
        cout << "," << *it;
    }
    cout << "];TotalCount=" << 0 << ";";
    cout << endl;

    indexSize.clear();

    #ifdef OUTPUT_X_AXIS
    noUpdate.clear();
    accumulate_cnt = 0;
    #endif
}

void run_swix(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{  
    int cnt = 0;
    
    pair<uint64_t, uint64_t> dataPair = make_pair(get<0>(data.front()),get<1>(data.front()));
    swix::SWmeta<uint64_t,uint64_t> swix(dataPair);

    for (auto it = data.begin()+1; it != data.begin()+TIME_WINDOW; ++it)
    {
        dataPair = make_pair(get<0>(*it),get<1>(*it));

        swix.insert(dataPair);

        #ifdef OUTPUT_X_AXIS
        ++accumulate_cnt;
        #endif

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            indexSize.push_back(swix.get_total_size_in_bytes());
            cnt = 0;

            #ifdef OUTPUT_X_AXIS
            noUpdate.push_back(accumulate_cnt);
            #endif
        }
    }

    uint64_t initialSize = swix.get_total_size_in_bytes();
    indexSize.push_back(swix.get_total_size_in_bytes());
    cnt = 0;

    #ifdef OUTPUT_X_AXIS
    noUpdate.push_back(accumulate_cnt);
    #endif

    auto it = data.begin()+TIME_WINDOW;
    auto itDelete = data.begin();

    uint64_t startTime = get<1>(*(data.begin()+TIME_WINDOW));

    if(startTime < 1)
    {
        cout << "ERROR : timestamp is less than 1 " << endl;
    }
    
    uint64_t lookupCount = 0;

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

            it++;

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

        #ifdef OUTPUT_X_AXIS
        ++accumulate_cnt;
        #endif

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            indexSize.push_back(swix.get_total_size_in_bytes());
            cnt = 0;

            #ifdef OUTPUT_X_AXIS
            noUpdate.push_back(accumulate_cnt);
            #endif
        }
    }

    uint64_t finalSize = swix.get_total_size_in_bytes();
    indexSize.push_back(swix.get_total_size_in_bytes());
    #ifdef OUTPUT_X_AXIS
    noUpdate.push_back(accumulate_cnt);
    #endif

    #ifdef TUNE
    cout << "Algorithm=SWIXTune";
    #else
    cout << "Algorithm=SWIX";
    #endif
    cout << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    if (LOAD_DATA_METHOD == 2)
    {
        cout << ";NoStandDev=" << NO_STD;
    }
    cout << ";Fanout=" << 0 << ";SplitError=" << INITIAL_ERROR << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";MapSize=0" << ";InitialSize=" << initialSize << ";FinalSize=" << finalSize;
    #ifdef OUTPUT_X_AXIS
    cout << ";X=[" << noUpdate.front();
    for (auto it = noUpdate.begin()+1; it != noUpdate.end(); ++it)
    {
        cout << "," << *it;
    }
    cout << "]";
    #endif
    cout << ";Size=[" << indexSize.front();
    for (auto it = indexSize.begin()+1; it != indexSize.end(); ++it)
    {
        cout << "," << *it;
    }
    cout << "];TotalCount=" << lookupCount << ";";
    cout << endl;

    indexSize.clear();

    #ifdef OUTPUT_X_AXIS
    noUpdate.clear();
    accumulate_cnt = 0;
    #endif
}


void run_alex(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    int cnt = 0;

    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    
    vector<pair<uint64_t, uint64_t>> data_initial_sorted = data_initial;
    sort(data_initial_sorted.begin(),data_initial_sorted.end());

    alex::Alex<uint64_t,uint64_t> alex;
    auto itInitial = data_initial_sorted.begin();
    auto itDeleteInitial = data_initial_sorted.begin();
    while (itInitial != data_initial_sorted.end())
    {
        uint64_t lowerLimit = ((double)get<1>(*itInitial) - TIME_WINDOW < 0)? 0 : get<1>(*itInitial)-TIME_WINDOW;
        while (get<1>(*itDeleteInitial) < lowerLimit)
        {
            alex.erase(get<0>(*itDeleteInitial));
            ++itDeleteInitial;
        }

        alex.insert(get<0>(*itInitial), get<1>(*itInitial));
        ++itInitial;

        #ifdef OUTPUT_X_AXIS
        ++accumulate_cnt;
        #endif

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            indexSize.push_back(alex::alex_get_total_size_in_bytes(alex));
            cnt = 0;

            #ifdef OUTPUT_X_AXIS
            noUpdate.push_back(accumulate_cnt);
            #endif
        }
    }

    uint64_t initialSize = alex::alex_get_total_size_in_bytes(alex);
    indexSize.push_back(alex::alex_get_total_size_in_bytes(alex));
    cnt = 0;

    #ifdef OUTPUT_X_AXIS
    noUpdate.push_back(accumulate_cnt);
    #endif

    auto it = data.begin()+TIME_WINDOW;
    auto itDelete = data.begin();
    
    uint64_t startTime = get<1>(*(data.begin()+TIME_WINDOW));

    if(startTime < 1)
    {
        cout << "ERROR : timestamp is less than 1 " << endl;
    }
    
    uint64_t lookupCount = 0;

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

            it++;
        }

        #ifdef OUTPUT_X_AXIS
        ++accumulate_cnt;
        #endif

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            indexSize.push_back(alex::alex_get_total_size_in_bytes(alex));
            cnt = 0;

            #ifdef OUTPUT_X_AXIS
            noUpdate.push_back(accumulate_cnt);
            #endif
        }
    }

    uint64_t finalSize = alex::alex_get_total_size_in_bytes(alex);
    indexSize.push_back(alex::alex_get_total_size_in_bytes(alex));
    #ifdef OUTPUT_X_AXIS
    noUpdate.push_back(accumulate_cnt);
    #endif

    cout << "Algorithm=Alex" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    if (LOAD_DATA_METHOD == 2)
    {
        cout << ";NoStandDev=" << NO_STD;
    }
    cout << ";Fanout=" << 0 << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";MapSize=" << size_of_map() << ";InitialSize=" << initialSize << ";FinalSize=" << finalSize;
    #ifdef OUTPUT_X_AXIS
    cout << ";X=[" << noUpdate.front();
    for (auto it = noUpdate.begin()+1; it != noUpdate.end(); ++it)
    {
        cout << "," << *it;
    }
    cout << "]";
    #endif
    cout << ";Size=[" << indexSize.front();
    for (auto it = indexSize.begin()+1; it != indexSize.end(); ++it)
    {
        cout << "," << *it;
    }
    cout << "];TotalCount=" << lookupCount << ";";
    cout << endl;

    indexSize.clear();

    #ifdef OUTPUT_X_AXIS
    noUpdate.clear();
    accumulate_cnt = 0;
    #endif
}


void run_pgm(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    int cnt = 0;
    pgm::DynamicPGMIndex<uint64_t,uint64_t,pgm::PGMIndex<uint64_t,FANOUT_BP>> pgm;

    auto it = data.begin();
    auto itDelete = data.begin();
    while (it != data.begin()+TIME_WINDOW)
    {
        uint64_t lowerLimit = ((double)get<1>(*it) - TIME_WINDOW < 0)? 0 : get<1>(*it)-TIME_WINDOW; 
        while (get<1>(*itDelete) < lowerLimit)
        {
            pgm.erase(get<0>(*itDelete));
            ++itDelete;
        }

        pgm.insert_or_assign(get<0>(*it),get<1>(*it));
        ++it;

        #ifdef OUTPUT_X_AXIS
        ++accumulate_cnt;
        #endif

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            indexSize.push_back(pgm::pgm_get_total_size_in_bytes(pgm));
            cnt = 0;

            #ifdef OUTPUT_X_AXIS
            noUpdate.push_back(accumulate_cnt);
            #endif
        }
    }

    uint64_t initialSize = pgm::pgm_get_total_size_in_bytes(pgm);
    indexSize.push_back(pgm::pgm_get_total_size_in_bytes(pgm));
    cnt = 0;

    #ifdef OUTPUT_X_AXIS
    noUpdate.push_back(accumulate_cnt);
    #endif

    it = data.begin()+TIME_WINDOW;
    itDelete = data.begin();

    uint64_t startTime = get<1>(*(data.begin()+TIME_WINDOW));

    if(startTime < 1)
    {
        cout << "ERROR : timestamp is less than 1 " << endl;
    }
    
    uint64_t lookupCount = 0;

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

            it++;
        }

        #ifdef OUTPUT_X_AXIS
        ++accumulate_cnt;
        #endif

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            indexSize.push_back(pgm::pgm_get_total_size_in_bytes(pgm));
            cnt = 0;

            #ifdef OUTPUT_X_AXIS
            noUpdate.push_back(accumulate_cnt);
            #endif
        }
    }
    
    uint64_t finalSize = pgm::pgm_get_total_size_in_bytes(pgm);
    indexSize.push_back(pgm::pgm_get_total_size_in_bytes(pgm));
    #ifdef OUTPUT_X_AXIS
    noUpdate.push_back(accumulate_cnt);
    #endif

    cout << "Algorithm=PGM" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    if (LOAD_DATA_METHOD == 2)
    {
        cout << ";NoStandDev=" << NO_STD;
    }
    cout << ";Fanout=" << FANOUT_BP << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";MapSize=" << size_of_map() << ";InitialSize=" << initialSize << ";FinalSize=" << finalSize;
    #ifdef OUTPUT_X_AXIS
    cout << ";X=[" << noUpdate.front();
    for (auto it = noUpdate.begin()+1; it != noUpdate.end(); ++it)
    {
        cout << "," << *it;
    }
    cout << "]";
    #endif
    cout << ";Size=[" << indexSize.front();
    for (auto it = indexSize.begin()+1; it != indexSize.end(); ++it)
    {
        cout << "," << *it;
    }
    cout << "];TotalCount=" << lookupCount << ";";
    cout << endl;

    indexSize.clear();

    #ifdef OUTPUT_X_AXIS
    noUpdate.clear();
    accumulate_cnt = 0;
    #endif
}

void run_btree(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    int cnt = 0;
    stx::btree_map<uint64_t,uint64_t,less<uint64_t>,btree::btree_traits_fanout<uint64_t>> btree;

    auto it = data.begin();
    auto itDelete = data.begin();
    while (it != data.begin()+TIME_WINDOW)
    {
        uint64_t lowerLimit = ((double)get<1>(*it) - TIME_WINDOW < 0)? 0 : get<1>(*it)-TIME_WINDOW;
        while (get<1>(*itDelete) < lowerLimit)
        {
            btree.erase(get<0>(*itDelete));
            ++itDelete;
        }

        pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));
        btree.insert(insertTuple);
        ++it;

        #ifdef OUTPUT_X_AXIS
        ++accumulate_cnt;
        #endif

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            indexSize.push_back(btree::bt_get_total_size_in_bytes(btree));
            cnt = 0;

            #ifdef OUTPUT_X_AXIS
            noUpdate.push_back(accumulate_cnt);
            #endif
        }
    }

    uint64_t initialSize = btree::bt_get_total_size_in_bytes(btree);
    indexSize.push_back(btree::bt_get_total_size_in_bytes(btree));
    cnt = 0;

    #ifdef OUTPUT_X_AXIS
    noUpdate.push_back(accumulate_cnt);
    #endif

    it = data.begin()+TIME_WINDOW;
    itDelete = data.begin();

    uint64_t startTime = get<1>(*(data.begin()+TIME_WINDOW));

    if(startTime < 1)
    {
        cout << "ERROR : timestamp is less than 1 " << endl;
    }
    
    uint64_t lookupCount = 0;

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

            it++;
        }

        #ifdef OUTPUT_X_AXIS
        ++accumulate_cnt;
        #endif

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            indexSize.push_back(btree::bt_get_total_size_in_bytes(btree));
            cnt = 0;

            #ifdef OUTPUT_X_AXIS
            noUpdate.push_back(accumulate_cnt);
            #endif

        }
    }

    uint64_t finalSize = btree::bt_get_total_size_in_bytes(btree);
    indexSize.push_back(btree::bt_get_total_size_in_bytes(btree));

    #ifdef OUTPUT_X_AXIS
    noUpdate.push_back(accumulate_cnt);
    #endif

    cout << "Algorithm=BTree" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    if (LOAD_DATA_METHOD == 2)
    {
        cout << ";NoStandDev=" << NO_STD;
    }
    cout << ";Fanout=" << FANOUT_BP << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";MapSize=" << size_of_map() << ";InitialSize=" << initialSize << ";FinalSize=" << finalSize;
     #ifdef OUTPUT_X_AXIS
    cout << ";X=[" << noUpdate.front();
    for (auto it = noUpdate.begin()+1; it != noUpdate.end(); ++it)
    {
        cout << "," << *it;
    }
    cout << "]";
    #endif
    cout << ";Size=[" << indexSize.front();
    for (auto it = indexSize.begin()+1; it != indexSize.end(); ++it)
    {
        cout << "," << *it;
    }
    cout << "];TotalCount=" << lookupCount << ";";
    cout << endl;

    indexSize.clear();

    #ifdef OUTPUT_X_AXIS
    noUpdate.clear();
    accumulate_cnt = 0;
    #endif
}

void run_imtree(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    int cnt = 0;
    imtree::IMTree<uint64_t,uint64_t> imtree(static_cast<int>((double)TIME_WINDOW*0.125));
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));
        imtree.insert(insertTuple);
        
        #ifdef OUTPUT_X_AXIS
        ++accumulate_cnt;
        #endif

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            indexSize.push_back(imtree.get_total_size_in_bytes());
            cnt = 0;

            #ifdef OUTPUT_X_AXIS
            noUpdate.push_back(accumulate_cnt);
            #endif
        }
    }

    uint64_t initialSize = imtree.get_total_size_in_bytes();
    indexSize.push_back(imtree.get_total_size_in_bytes());
    cnt = 0;

    #ifdef OUTPUT_X_AXIS
    noUpdate.push_back(accumulate_cnt);
    #endif

    auto it = data.begin()+TIME_WINDOW;
    auto itDelete = data.begin();

    uint64_t startTime = get<1>(*(data.begin()+TIME_WINDOW));

    if(startTime < 1)
    {
        cout << "ERROR : timestamp is less than 1 " << endl;
    }

    uint64_t lookupCount = 0;

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

            it++;
        }

        #ifdef OUTPUT_X_AXIS
        ++accumulate_cnt;
        #endif

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            indexSize.push_back(imtree.get_total_size_in_bytes());
            cnt = 0;

            #ifdef OUTPUT_X_AXIS
            noUpdate.push_back(accumulate_cnt);
            #endif
        }
    }

    uint64_t finalSize = imtree.get_total_size_in_bytes();
    indexSize.push_back(imtree.get_total_size_in_bytes());

    #ifdef OUTPUT_X_AXIS
    noUpdate.push_back(accumulate_cnt);
    #endif

    cout << "Algorithm=IMTree" << ";Data=" << FILE_NAME << ";MatchRate=" << MATCH_RATE;
    if (LOAD_DATA_METHOD == 2)
    {
        cout << ";NoStandDev=" << NO_STD;
    }
    cout << ";Fanout=" << 0 << ";SplitError=" << 0 << ";TimeWindow=" << TIME_WINDOW << ";UpdateLength=" << (TEST_LEN/TIME_WINDOW)-1;
    cout << ";MapSize=0" << ";InitialSize=" << initialSize << ";FinalSize=" << finalSize;
    #ifdef OUTPUT_X_AXIS
    cout << ";X=[" << noUpdate.front();
    for (auto it = noUpdate.begin()+1; it != noUpdate.end(); ++it)
    {
        cout << "," << *it;
    }
    cout << "]";
    #endif
    cout << ";Size=[" << indexSize.front();
    for (auto it = indexSize.begin()+1; it != indexSize.end(); ++it)
    {
        cout << "," << *it;
    }
    cout << "];TotalCount=" << lookupCount << ";";
    cout << endl;

    indexSize.clear();

    #ifdef OUTPUT_X_AXIS
    noUpdate.clear();
    accumulate_cnt = 0;
    #endif
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
    run_vector(data);
    #endif;

    return 0;
}