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
#include "../src/FLIRT.hpp"
#include "../utils/load.hpp"
#include "../timer/rdtsc.h"

//For comparing memory usage with FLIRT

using namespace std;

#ifndef SNAP_SHOT
#define SNAP_SHOT TIME_WINDOW * 0.5
#endif

#ifndef NO_SEGMENT //For Flirt
#define NO_SEGMENT TIME_WINDOW
#endif

uint64_t initialSizeVector, finalSizeVector, maxSizeVector = 0.00001;
double averageSizeVector = 0.00001;

void load_data(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    std::string input_file = DATA_DIR FILE_NAME;

    add_timestamp_append(input_file, data, MATCH_RATE, SEED);
}

void run_vector(vector<tuple<uint64_t, uint64_t, uint64_t>> & data, bool print = false)
{
    int cnt = 0;
    int snapShotCount = 0;

    initialSizeVector = sizeof(vector<pair<uint64_t, uint64_t>>) + sizeof(pair<uint64_t, uint64_t>) * TIME_WINDOW;

    auto it = data.begin()+TIME_WINDOW;
    auto itDelete = data.begin();

    uint64_t startTime = get<1>(*(data.begin()+TIME_WINDOW));

    if(startTime < 1)
    {
        cout << "ERROR : timestamp is less than 1 " << endl;
    }

    uint64_t maxTimestamp =  get<1>(data.back());
    averageSizeVector = 0;
    maxSizeVector = 0;

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

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            uint64_t tempSize = sizeof(vector<pair<uint64_t, uint64_t>>) + sizeof(pair<uint64_t, uint64_t>) * (it - itDelete);
            averageSizeVector += tempSize;
            if (maxSizeVector < tempSize) {maxSizeVector = tempSize;}
            ++snapShotCount;
            cnt = 0;
        }
    }

    finalSizeVector = sizeof(vector<pair<uint64_t, uint64_t>>) + sizeof(pair<uint64_t, uint64_t>) * (it - itDelete);
    averageSizeVector /= snapShotCount;

    if (print)
    {
        cout << "Algorithm=Vector";
        cout << ", Data=" << FILE_NAME << ", MatchRate=" << MATCH_RATE << ", Fanout=0";
        cout << ", Initial=" << initialSizeVector << "(" << 0 << ")";
        cout << ", Final=" << finalSizeVector << "(" << 0 << ")";
        cout << ", Average=" << averageSizeVector << "(" << 0 << ")";
        cout << ", Max=" << maxSizeVector << "(" << 0 << ")";
        cout << ", TotalCount=" << 0;
        cout << endl; 
    }
}

void run_vector_1d(vector<tuple<uint64_t, uint64_t, uint64_t>> & data, bool print = false)
{
    int cnt = 0;
    int snapShotCount = 0;

    initialSizeVector = sizeof(vector<uint64_t>) + sizeof(uint64_t) * TIME_WINDOW;

    auto it = data.begin()+TIME_WINDOW;
    auto itDelete = data.begin();

    uint64_t startTime = get<1>(*(data.begin()+TIME_WINDOW));

    if(startTime < 1)
    {
        cout << "ERROR : timestamp is less than 1 " << endl;
    }

    uint64_t maxTimestamp =  get<1>(data.back());
    averageSizeVector = 0;
    maxSizeVector = 0;

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

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            uint64_t tempSize = sizeof(vector<uint64_t>) + sizeof(uint64_t) * (it - itDelete);
            averageSizeVector += tempSize;
            if (maxSizeVector < tempSize) {maxSizeVector = tempSize;}
            ++snapShotCount;
            cnt = 0;
        }
    }

    finalSizeVector = sizeof(vector<uint64_t>) + sizeof(uint64_t) * (it - itDelete);
    averageSizeVector /= snapShotCount;

    if (print)
    {
        cout << "Algorithm=Vector1D";
        cout << ", Data=" << FILE_NAME << ", MatchRate=" << MATCH_RATE << ", Fanout=0";
        cout << ", Initial=" << initialSizeVector << "(" << 0 << ")";
        cout << ", Final=" << finalSizeVector << "(" << 0 << ")";
        cout << ", Average=" << averageSizeVector << "(" << 0 << ")";
        cout << ", Max=" << maxSizeVector << "(" << 0 << ")";
        cout << ", TotalCount=" << 0;
        cout << endl; 
    }
}


void run_swix(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    int cnt = 0;
    int snapShotCount = 0;
    double averageSize = 0;
    uint64_t maxSize = 0;
    
    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    
    swix::SWmeta<uint64_t,uint64_t> swix(data_initial);
    uint64_t initialSize = swix.get_total_size_in_bytes();

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
            swix.range_search_ordered_data(searchTuple,tempJoinResult);
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

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            uint64_t tempSize = swix.get_total_size_in_bytes();
            averageSize += tempSize;
            if (maxSize < tempSize) {maxSize = tempSize;}
            ++snapShotCount;
            cnt = 0;
        }
    }

    uint64_t finalSize = swix.get_total_size_in_bytes();
    averageSize /= snapShotCount;

    #ifdef TUNE
    cout << "Algorithm=SWIXTune";
    #else
    cout << "Algorithm=SWIX";
    #endif
    cout << ", Data=" << FILE_NAME << ", MatchRate=" << MATCH_RATE << ", Fanout=" << INITIAL_ERROR;
    cout << ", Initial=" << initialSize << "(" << ((double)initialSize - (double)initialSizeVector)/(double)initialSizeVector*100 << ")";
    cout << ", Final=" << finalSize << "(" << ((double)finalSize - (double)finalSizeVector)/(double)finalSizeVector*100 << ")";
    cout << ", Average=" << averageSize << "(" << (averageSize-averageSizeVector)/averageSizeVector*100 << ")";
    cout << ", Max=" << maxSize << "(" << ((double)maxSize - (double)maxSizeVector)/(double)maxSizeVector*100 << ")";
    cout << ", TotalCount=" << lookupCount;
    cout << endl;
}


void run_alex(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    int cnt = 0;
    int snapShotCount = 0;
    double averageSize = 0;
    uint64_t maxSize = 0;

    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    
    vector<pair<uint64_t, uint64_t>> data_initial_sorted = data_initial;
    sort(data_initial_sorted.begin(),data_initial_sorted.end());

    alex::Alex<uint64_t,uint64_t> alex;
    for (auto it = data_initial_sorted.begin(); it != data_initial_sorted.end(); it++)
    {
        alex.insert(it->first, it->second);
    }

    uint64_t initialSize = alex::alex_get_total_size_in_bytes(alex);

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

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            uint64_t tempSize = alex::alex_get_total_size_in_bytes(alex);
            averageSize += tempSize;
            if (maxSize < tempSize) {maxSize = tempSize;}
            ++snapShotCount;
            cnt = 0;
        }
    }

    uint64_t finalSize = alex::alex_get_total_size_in_bytes(alex);
    averageSize /= snapShotCount;

    cout << "Algorithm=Alex";
    cout << ", Data=" << FILE_NAME << ", MatchRate=" << MATCH_RATE << ", Fanout=0";
    cout << ", Initial=" << initialSize << "(" << ((double)initialSize - (double)initialSizeVector)/(double)initialSizeVector*100 << ")";
    cout << ", Final=" << finalSize << "(" << ((double)finalSize - (double)finalSizeVector)/(double)finalSizeVector*100 << ")";
    cout << ", Average=" << averageSize << "(" << (averageSize-averageSizeVector)/averageSizeVector*100 << ")";
    cout << ", Max=" << maxSize << "(" << ((double)maxSize - (double)maxSizeVector)/(double)maxSizeVector*100 << ")";
    cout << ", TotalCount=" << lookupCount;
    cout << endl;

}


void run_pgm(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    int cnt = 0;
    int snapShotCount = 0;
    double averageSize = 0;
    uint64_t maxSize = 0;

    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    
    vector<pair<uint64_t, uint64_t>> data_initial_sorted = data_initial;
    sort(data_initial_sorted.begin(),data_initial_sorted.end());

    pgm::DynamicPGMIndex<uint64_t,uint64_t,pgm::PGMIndex<uint64_t,FANOUT_BP>> pgm(data_initial_sorted.begin(),data_initial_sorted.end());

    uint64_t initialSize = pgm::pgm_get_total_size_in_bytes(pgm);

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

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            uint64_t tempSize = pgm::pgm_get_total_size_in_bytes(pgm);
            averageSize += tempSize;
            if (maxSize < tempSize) {maxSize = tempSize;}
            ++snapShotCount;
            cnt = 0;
        }
    }

    uint64_t finalSize = pgm::pgm_get_total_size_in_bytes(pgm);
    averageSize /= snapShotCount;

    cout << "Algorithm=PGM";
    cout << ", Data=" << FILE_NAME << ", MatchRate=" << MATCH_RATE << ", Fanout=" << FANOUT_BP;
    cout << ", Initial=" << initialSize << "(" << ((double)initialSize - (double)initialSizeVector)/(double)initialSizeVector*100 << ")";
    cout << ", Final=" << finalSize << "(" << ((double)finalSize - (double)finalSizeVector)/(double)finalSizeVector*100 << ")";
    cout << ", Average=" << averageSize << "(" << (averageSize-averageSizeVector)/averageSizeVector*100 << ")";
    cout << ", Max=" << maxSize << "(" << ((double)maxSize - (double)maxSizeVector)/(double)maxSizeVector*100 << ")";
    cout << ", TotalCount=" << lookupCount;
    cout << endl;
}

void run_btree(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    int cnt = 0;
    int snapShotCount = 0;
    double averageSize = 0;
    uint64_t maxSize = 0;

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

    auto it = data.begin()+TIME_WINDOW;
    auto itDelete = data.begin();

    uint64_t startTime = get<1>(*(data.begin()+TIME_WINDOW));

    if(startTime < 1)
    {
        cout << "ERROR : timestamp is less than 1 " << endl;
    }
    
    uint64_t loopCounter = 0;
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
            loopCounter++;
        }

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            uint64_t tempSize = btree::bt_get_total_size_in_bytes(btree);
            averageSize += tempSize;
            if (maxSize < tempSize) {maxSize = tempSize;}
            ++snapShotCount;
            cnt = 0;
        }
    }

    uint64_t finalSize = btree::bt_get_total_size_in_bytes(btree);
    averageSize /= snapShotCount;

    cout << "Algorithm=BTree";
    cout << ", Data=" << FILE_NAME << ", MatchRate=" << MATCH_RATE << ", Fanout=" << FANOUT_BP;
    cout << ", Initial=" << initialSize << "(" << ((double)initialSize - (double)initialSizeVector)/(double)initialSizeVector*100 << ")";
    cout << ", Final=" << finalSize << "(" << ((double)finalSize - (double)finalSizeVector)/(double)finalSizeVector*100 << ")";
    cout << ", Average=" << averageSize << "(" << (averageSize-averageSizeVector)/averageSizeVector*100 << ")";
    cout << ", Max=" << maxSize << "(" << ((double)maxSize - (double)maxSizeVector)/(double)maxSizeVector*100 << ")";
    cout << ", TotalCount=" << lookupCount;
    cout << endl;
}

void run_imtree(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    int cnt = 0;
    int snapShotCount = 0;
    double averageSize = 0;
    uint64_t maxSize = 0;

    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    
    imtree::IMTree<uint64_t,uint64_t> imtree(data_initial,static_cast<int>((double)TIME_WINDOW*0.125));

    uint64_t initialSize = imtree.get_total_size_in_bytes();

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

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            uint64_t tempSize = imtree.get_total_size_in_bytes();
            averageSize += tempSize;
            if (maxSize < tempSize) {maxSize = tempSize;}
            ++snapShotCount;
            cnt = 0;
        }
    }

    uint64_t finalSize = imtree.get_total_size_in_bytes();
    averageSize /= snapShotCount;

    cout << "Algorithm=IMTree";
    cout << ", Data=" << FILE_NAME << ", MatchRate=" << MATCH_RATE << ", Fanout=0";
    cout << ", Initial=" << initialSize << "(" << ((double)initialSize - (double)initialSizeVector)/(double)initialSizeVector*100 << ")";
    cout << ", Final=" << finalSize << "(" << ((double)finalSize - (double)finalSizeVector)/(double)finalSizeVector*100 << ")";
    cout << ", Average=" << averageSize << "(" << (averageSize-averageSizeVector)/averageSizeVector*100 << ")";
    cout << ", Max=" << maxSize << "(" << ((double)maxSize - (double)maxSizeVector)/(double)maxSizeVector*100 << ")";
    cout << ", TotalCount=" << lookupCount;
    cout << endl;
}


void run_flirt(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    int cnt = 0;
    int snapShotCount = 0;
    double averageSize = 0;
    uint64_t maxSize = 0;
    
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
    
    uint64_t initialSize = flirt.get_total_size_in_bytes();

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
            flirt.dequeue();
            itDelete++;
        }

        while(get<1>(*it) == i)
        {
            tuple<uint64_t,uint64_t,uint64_t> searchTuple = data.at((itDelete - data.begin()) + (rand() % ( (it - data.begin()) - (itDelete - data.begin()) + 1 )));
            pair<uint64_t,uint64_t> insertTuple = make_pair(get<0>(*it),get<1>(*it));

            vector<uint64_t> tempJoinResult;
            flirt.range_search(get<0>(searchTuple), get<0>(searchTuple), get<2>(searchTuple), tempJoinResult);
            lookupCount += tempJoinResult.size();

            #ifdef TUNE
            flirt.enqueue_auto_tune(insertTuple.first);
            #else
            flirt.enqueue(insertTuple.first);
            #endif

            it++;
        }

        ++cnt;
        if (cnt == SNAP_SHOT)
        {
            uint64_t tempSize = flirt.get_total_size_in_bytes();
            averageSize += tempSize;
            if (maxSize < tempSize) {maxSize = tempSize;}
            ++snapShotCount;
            cnt = 0;
        }
    }

    uint64_t finalSize = flirt.get_total_size_in_bytes();
    averageSize /= snapShotCount;

    #ifdef TUNE
    cout << "Algorithm=FLIRTAutoTune";
    #else
    cout << "Algorithm=FLIRT";
    #endif 
    cout << ", Data=" << FILE_NAME << ", MatchRate=" << MATCH_RATE << ", Fanout=" << INITIAL_ERROR;
    cout << ", Initial=" << initialSize << "(" << ((double)initialSize - (double)initialSizeVector)/(double)initialSizeVector*100 << ")";
    cout << ", Final=" << finalSize << "(" << ((double)finalSize - (double)finalSizeVector)/(double)finalSizeVector*100 << ")";
    cout << ", Average=" << averageSize << "(" << (averageSize-averageSizeVector)/averageSizeVector*100 << ")";
    cout << ", Max=" << maxSize << "(" << ((double)maxSize - (double)maxSizeVector)/(double)maxSizeVector*100 << ")";
    cout << ", TotalCount=" << lookupCount;
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
        case 1: //Specific case for PGM & B+Tree as they depend on Fanout
            run_vector(data);
            run_pgm(data);
            run_btree(data);
            break;

        case 2: //Run Flirt with 1D data (NO_SEGMENT for Flirt can change)
            run_vector_1d(data,true);
            run_flirt(data);
            break;

        case 3: //Run SWIX
            run_vector(data,true);
            run_swix(data);
            break;

        default: //All other cases
            run_vector(data,true);
            run_swix(data);
            run_alex(data);
            run_imtree(data);
            break;
    }
    #else
    run_vector(data,true);
    run_swix(data);
    run_alex(data);
    run_pgm(data);
    run_btree(data);
    run_imtree(data);
    run_vector_1d(data,true);
    run_flirt(data);
    #endif

    return 0;
}