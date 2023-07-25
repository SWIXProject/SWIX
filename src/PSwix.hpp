#ifndef __PSWIX_META_HPP__
#define __PSWIX_META_HPP__

#pragma once
#include <atomic>
#include <mutex>
#include <condition_variable>
#include "../lib/multithread_queues/concurrent_queue.h"

#include "PSWseg.hpp"

using namespace std;

namespace pswix {

typedef uint64_t key_type;
typedef uint64_t time_type;

typedef tuple<bool,int,int,int> search_bound_type;
typedef tuple<task_status,bool,key_type,SWseg<key_type,time_type>*> meta_workload_type;

mutex thread_lock[NUM_THREADS-1];
condition_variable thread_cv[NUM_THREADS-1];
bool thread_occupied[NUM_THREADS-1];

moodycamel::ConcurrentQueue<meta_workload_type> meta_queue(NUM_SEARCH_PER_ROUND + NUM_UPDATE_PER_ROUND*2);

template<class Type_Key, class Type_Ts> 
class SWmeta
{
//Variables
private:
    int splitError;

    atomic<int> m_rightSearchBound;
    atomic<int> m_leftSearchBound;
    atomic<int> m_maxSearchError;
    atomic<int> m_numSeg;
    atomic<int> m_numSegExist;
    
    Type_Key m_startKey;
    double m_slope;

    vector<int> m_partitionIndex; //start index of each partition
    vector<Type_Ts> m_parititonMaxTime; //Updated during insertion
    vector<atomic<Type_Key>> m_partitionStartKey; //starting key of each partition
    vector<atomic<Type_Key>> m_partitionEndKey; //this value is the value of the last key in meta for each partition (may be gap) - Used during insertion
    
    vector<uint64_t> m_bitmap;
    vector<uint64_t> m_retrainBitmap;
    vector<Type_Key> m_keys;
    vector<SWseg<Type_Key,Type_Ts>*> m_ptr;

//Functions
public:
    //Constructors & Deconstructors
    SWmeta();
    SWmeta(int numThreads, const pair<Type_Key, Type_Ts> & arrivalTuple);
    SWmeta(int numThreads, const vector<pair<Type_Key, Type_Ts>> & stream);
    ~SWmeta();

    //Bulk Load
    void bulk_load(int numThreads, const vector<pair<Type_Key, Type_Ts>> & stream);
private:
    void split_data(const vector<pair<Type_Key,Type_Ts>> & data, vector<pair<Type_Key, SWseg<Type_Key,Type_Ts> * >> & splitedDataPtr);

public:
    //Multithread Index Operations
    void partition_threads(int numThreads);
    vector<uint32_t> predict_thread(Type_Key key);
    vector<uint32_t> predict_thread(Type_Key key, tuple<bool,int,int,int> & predictBound);
    vector<uint32_t> predict_thread(Type_Key key, vector<tuple<bool,int,int,int>> & predictBound);
    vector<uint32_t> predict_thread(Type_Key lowerBound, Type_Key upperBound, vector<tuple<bool,int,int,int>> & predictBound);

    bool within_thread(uint32_t threadID, Type_Key key, tuple<bool,int,int,int> & predictBound);
    bool within_thread(uint32_t threadID, Type_Key lowerBound, Type_Key upperBound, tuple<bool,int,int,int> & predictBound);

    int lookup(uint32_t threadID, Type_Key key, Type_Ts timestamp, tuple<bool,int,int,int> & predictBound);

    int range_query(uint32_t threadID, Type_Key lowerBound, Type_Ts timestamp, Type_Key upperBound, tuple<bool,int,int,int> & predictBound);

    int insert(uint32_t threadID, Type_Key key, Type_Ts timestamp, tuple<bool,int,int,int> & predictBound);

private:
    //Search helpers
    int meta_model_search(uint32_t threadID, Type_Key targetKey, tuple<bool,int,int,int> & predictBound);
    int meta_non_model_search(uint32_t threadID, Type_Key targetKey, tuple<bool,int,int,int> & searchBound);

    //Update SWmeta helper functions
    int thread_dispatch_update_seg(uint32_t threadID, Type_Ts expiryTime, 
                                    vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg,
                                    vector<vector<pair<Type_Key,Type_Ts>>> & retrainData);
    vector<pair<Type_Key,Type_Ts>> seg_merge_data(uint32_t threadID, int startIndex, int endIndex, Type_Ts expiryTime);
    void seg_retrain(uint32_t threadID, int startIndex, int endIndex, Type_Ts expiryTime);
    void seg_retrain(uint32_t threadID, vector<pair<Type_Key,Type_Ts>> & data,
                    vector<tuple<Type_Key,SWseg<Type_Key,Type_Ts>*,bool>> & retrainSeg);

public:
    //Meta thread insertions
    int meta_insert(uint32_t threadID, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool currentRetrainStatus);

private:
    //Internal insertion function
    int thread_insert(uint32_t threadID, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool currentRetrainStatus);

    //Insertion helpers functions
    int insert_model(uint32_t threadID, int insertionPos, int gapPos, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool addErrorFlag, bool currentRetrainStatus);
    int insert_end(Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool currentRetrainStatus);
    int insert_append(int insertionPos, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool currentRetrainStatus);

    void meta_synchroize_thread_for_insert(uint32_t startThreadID, uint32_t endThreadID, vector<bool> & threadLockStatus, vector<unique_lock<mutex>> & locks);

public: 
    // Meta retrain
    void meta_retrain();

private: 
    //Util helper functions
    tuple<int,int,int> find_predict_pos_bound(Type_Key targetKey);
    void exponential_search_right(Type_Key targetKey, int & foundPos, int maxSearchBound);
    void exponential_search_right_insert(Type_Key targetKey, int & foundPos, int maxSearchBound);
    void exponential_search_left(Type_Key targetKey, int & foundPos, int maxSearchBound);
    void exponential_search_left_insert(Type_Key targetKey, int & foundPos, int maxSearchBound);

    uint32_t find_index_partition(int index);
    int find_first_segment_in_partition(uint32_t threadID);
    int find_last_segment_in_partition(uint32_t threadID);

    int find_closest_gap(int index);
    int find_closest_gap(int index, int leftBoundary, int rightBoundary);
    int find_closest_gap_within_thread(uint32_t threadID, int index);

    pair<int,int> find_neighbours(int index, int leftBoundary, int rightBoundary);
    void update_segment_with_neighbours(int index, int leftBoundary, int rightBoundary);

    void update_keys_with_previous(uint32_t threadID, int index);
    void update_partition_start_end_key(uint32_t threadID);
    void update_partition_starting_gap_key(uint32_t threadID);

    void lock_thread(uint32_t threadID, unique_lock<mutex> & lock);
    void unlock_thread(uint32_t threadID, unique_lock<mutex> & lock);

public:
    //Getters & Setters
    size_t get_meta_size();
    size_t get_no_seg();
    int get_split_error();
    void set_split_error(int error);
    void print();
    void print_all();
    uint64_t get_total_size_in_bytes();
    uint64_t get_no_keys(Type_Ts Timestamp);

private:
    //Bitmap functions
    bool bitmap_exists(int index) const;
    bool bitmap_exists(vector<uint64_t> & bitmap, int index) const;
    void bitmap_set_bit(int index);
    void bitmap_set_bit(vector<uint64_t> & bitmap, int index);
    void bitmap_erase_bit(int index);
    void bitmap_erase_bit(vector<uint64_t> & bitmap, int index);

    int bitmap_closest_left_nongap(int index, int leftBoundary);
    int bitmap_closest_right_nongap(int index, int rightBoundary);
    int bitmap_closest_gap(int index);
    int bitmap_closest_gap(int index, int leftBoundary, int rightBoundary);
    pair<int,int> bitmap_retrain_range(int index);
    void bitmap_move_bit_back(int startingIndex, int endingIndex);
    void bitmap_move_bit_back(vector<uint64_t> & bitmap, int startingIndex, int endingIndex);
    void bitmap_move_bit_front(int startingIndex, int endingIndex);
    void bitmap_move_bit_front(vector<uint64_t> & bitmap, int startingIndex, int endingIndex);
};

/*
Constructors & Deconstructors
*/
template<class Type_Key, class Type_Ts> 
SWmeta<Type_Key,Type_Ts>::SWmeta()
:m_rightSearchBound(0), m_leftSearchBound(0), m_numSeg(0), m_numSegExist(0), splitError(0), m_slope(-1), m_maxSearchError(0), 
 m_startKey(numeric_limits<Type_Key>::min()){}

template<class Type_Key, class Type_Ts> 
SWmeta<Type_Key,Type_Ts>::SWmeta(int numThreads, const pair<Type_Key, Type_Ts> & arrivalTuple)
:m_rightSearchBound(0), m_leftSearchBound(0), m_numSeg(0), m_numSegExist(0), m_slope(-1), m_maxSearchError(0), m_startKey(0)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","Constructor(singleData)");
    #endif

    SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(arrivalTuple);
    SWsegPtr->m_leftSibling = nullptr;
    SWsegPtr->m_rightSibling = nullptr;
    SWsegPtr->m_parentIndex = 0;

    m_keys.push_back(arrivalTuple.first);
    m_ptr.push_back(SWsegPtr);
    // m_partitionKeys.push_back({arrivalTuple.first});
    // m_partitionPtr.push_back({SWsegPtr});

    m_startKey = arrivalTuple.first;
    m_numSeg = 1;
    m_numSegExist = 1;
    m_bitmap.push_back(0);
    m_retrainBitmap.push_back(0);
    bitmap_set_bit(0);

    splitError = 64;

    partition_threads(numThreads);

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","Constructor(singleData)");
    #endif
}

template<class Type_Key, class Type_Ts> 
SWmeta<Type_Key,Type_Ts>::SWmeta(int numThreads, const vector<pair<Type_Key, Type_Ts>> & stream)
:m_rightSearchBound(0), m_leftSearchBound(0), m_numSeg(0), m_numSegExist(0), m_slope(0), m_maxSearchError(0)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","Constructor(data)");
    #endif

    bulk_load(numThreads, stream);
    splitError = 64;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","Constructor(data)");
    #endif
}

template<class Type_Key, class Type_Ts> 
SWmeta<Type_Key,Type_Ts>::~SWmeta()
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","Deconstructor");
    #endif

    if (m_ptr.size() == 0)
    {
        return;
    }
    
    for (auto & it: m_ptr)
    {
        if (it != nullptr)
        {
            delete it;
            it = nullptr;
        }
    }

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","Deconstructor");
    #endif
}

/*
Bulk load & Retrain
*/
template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::bulk_load(int numThreads, const vector<pair<Type_Key, Type_Ts>> & stream)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","bulk_load");
    #endif

    vector<pair<Type_Key, Type_Ts>> data(stream);

    sort(data.begin(), data.end());

    vector<pair<Type_Key, SWseg<Type_Key,Type_Ts> * >> splitedDataPtr;

    split_data(data, splitedDataPtr);

    if (m_slope != -1)
    {
        int currentIndex = 1;
        int currentInsertionPos = 0;
        int predictedPos;

        splitedDataPtr[0].second->m_parentIndex = currentInsertionPos;
        currentInsertionPos++;

        m_keys.push_back(splitedDataPtr[0].first);
        m_ptr.push_back(splitedDataPtr[0].second);
        m_startKey = splitedDataPtr[0].first;
        m_numSegExist = splitedDataPtr.size();
        m_bitmap.push_back(0);
        m_retrainBitmap.push_back(0);
        bitmap_set_bit(0);

        while (currentIndex < splitedDataPtr.size())
        {
            predictedPos = static_cast<int>(floor(m_slope * ((double)splitedDataPtr[currentIndex].first - (double)m_startKey)));

            if (static_cast<int>(currentInsertionPos >> 6) == m_bitmap.size())
            {
                m_bitmap.push_back(0);
                m_retrainBitmap.push_back(0);
            }

            if(predictedPos == currentInsertionPos)
            {
                splitedDataPtr[currentIndex].second->m_parentIndex = currentInsertionPos;
                
                m_keys.push_back(splitedDataPtr[currentIndex].first);
                m_ptr.push_back(splitedDataPtr[currentIndex].second);
                bitmap_set_bit(currentInsertionPos);

                ++currentIndex;
            }
            else if (predictedPos < currentInsertionPos)
            {
                splitedDataPtr[currentIndex].second->m_parentIndex = currentInsertionPos;
                
                m_keys.push_back(splitedDataPtr[currentIndex].first);
                m_ptr.push_back(splitedDataPtr[currentIndex].second);
                bitmap_set_bit(currentInsertionPos);

                m_rightSearchBound = (currentInsertionPos - predictedPos) > m_rightSearchBound.load() 
                                    ? (currentInsertionPos - predictedPos) : m_rightSearchBound.load();
                
                ++currentIndex;
            }
            else
            {
                m_keys.push_back(m_keys.back());
                m_ptr.push_back(nullptr);
            }
            ++currentInsertionPos;
        }   
        m_rightSearchBound += 1;
        m_numSeg = m_keys.size();
        m_maxSearchError = min(8192,(int)ceil(0.6*m_numSeg));
    }
    else //Single Segment in SWmeta
    {        
        int bitmapSize = ceil(MAX_BUFFER_SIZE/64.);
        vector<uint64_t> temp(bitmapSize,0);
        
        m_startKey = splitedDataPtr[0].first;
        m_numSeg = 1;
        m_numSegExist = 1;
        m_bitmap = temp;
        m_retrainBitmap = temp;
        bitmap_set_bit(0);
        
        splitedDataPtr[0].second->m_parentIndex = 0;
        m_keys.push_back(splitedDataPtr[0].first);
        m_ptr.push_back(splitedDataPtr[0].second);
    }

    partition_threads(numThreads);

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","bulk_load");
    #endif
}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::split_data(const vector<pair<Type_Key,Type_Ts>> & data, 
                                vector<pair<Type_Key, SWseg<Type_Key,Type_Ts> * >> & splitedDataPtr)
{  
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","split_data");
    #endif

    vector<tuple<int,int,double>> splitIndexSlopeVector;
    calculate_split_derivative((int)(TIME_WINDOW * 0.00001), data, splitIndexSlopeVector);

    if (splitIndexSlopeVector.size() != 1)
    {
        Type_Key firstKey = data[get<0>(splitIndexSlopeVector.front())].first;
        double normalizedKey = 0, sumKey = 0, sumIndex = 0, sumKeyIndex = 0, sumKeySquared = 0;
        int cnt = 0;
        
        for (auto it = splitIndexSlopeVector.begin(); it != splitIndexSlopeVector.end()-1; it++)
        {
            SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(get<0>(*it), get<1>(*it) ,get<2>(*it), data);
            if(splitedDataPtr.size() > 0)
            {
                SWsegPtr->m_leftSibling = splitedDataPtr[splitedDataPtr.size() - 1].second;
                splitedDataPtr[splitedDataPtr.size() - 1].second->m_rightSibling = SWsegPtr;
            }
            splitedDataPtr.push_back(make_pair( data[get<0>(*it)].first, SWsegPtr));

            normalizedKey = (double)data[get<0>(*it)].first - (double)firstKey;
            sumKey += normalizedKey;
            sumIndex += cnt;
            sumKeyIndex += normalizedKey * cnt;
            sumKeySquared += pow(normalizedKey,2);
            ++cnt;
        }

        //Dealing with last segment with only one point
        if (get<0>(splitIndexSlopeVector.back())  != get<1>(splitIndexSlopeVector.back()))
        {
            SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(get<0>(splitIndexSlopeVector.back()), get<1>(splitIndexSlopeVector.back()) ,get<2>(splitIndexSlopeVector.back()), data);
            if(splitedDataPtr.size() > 0)
            {
                SWsegPtr->m_leftSibling = splitedDataPtr[splitedDataPtr.size() - 1].second;
                splitedDataPtr[splitedDataPtr.size() - 1].second->m_rightSibling = SWsegPtr;
            }
            splitedDataPtr.push_back(make_pair( data[get<0>(splitIndexSlopeVector.back())].first, SWsegPtr));

            normalizedKey = (double)data[get<0>(splitIndexSlopeVector.back())].first - (double)firstKey;
            sumKey += normalizedKey;
            sumIndex += cnt;
            sumKeyIndex += normalizedKey * cnt;
            sumKeySquared += pow(normalizedKey,2);
            ++cnt;
        }
        else //Single Point 
        {        
            SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(data.back());
            if(splitedDataPtr.size() > 0)
            {
                SWsegPtr->m_leftSibling = splitedDataPtr[splitedDataPtr.size() - 1].second;
                splitedDataPtr[splitedDataPtr.size() - 1].second->m_rightSibling = SWsegPtr;
            }
            splitedDataPtr.push_back(make_pair( data.back().first, SWsegPtr));

            normalizedKey = (double)data.back().first - (double)firstKey;
            sumKey += normalizedKey;
            sumIndex += cnt;
            sumKeyIndex += normalizedKey * cnt;
            sumKeySquared += pow(normalizedKey,2);
            ++cnt;
        }

        m_slope =  (sumKeyIndex - sumKey *(sumIndex/cnt))/(sumKeySquared - sumKey*(sumKey/cnt)) * 1.05;
    }
    else //Single Segment in SWmeta
    {
        if (get<0>(splitIndexSlopeVector[0]) != get<1>(splitIndexSlopeVector[0]))
        {
            SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(get<0>(splitIndexSlopeVector[0]), get<1>(splitIndexSlopeVector[0]), get<2>(splitIndexSlopeVector[0]), data);
            splitedDataPtr.push_back(make_pair( data[get<0>(splitIndexSlopeVector[0])].first, SWsegPtr));
        }
        else
        {
            printf("WARNING: entire data is one point \n");
            SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(data.back());
            splitedDataPtr.push_back(make_pair( data.back().first, SWsegPtr));
        }
        m_slope = -1;
    }

    if (splitedDataPtr.front().second->m_leftSibling != nullptr)
    {
        printf("WARNING: left sibling of first segment not null \n");
    }

    if (splitedDataPtr.back().second->m_rightSibling != nullptr)
    {
        printf("WARNING: right sibling of last segmentnot null \n");
    }

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","split_data");
    #endif
}

/*
Parition data for threads
*/
template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::partition_threads(int numThreads)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","partition_threads");
    #endif

    if (m_numSeg == 1 || m_slope == -1)
    {
        printf("WARNING: only one segment in data, going into single thread mode \n");
        m_partitionIndex.push_back(0);
        m_parititonMaxTime.push_back(0);
    }
    else
    {
        //Equal Split Segments into Threads (Simple Partitioning)
        int numSegmentsPerThread = m_numSeg / numThreads;

        m_partitionIndex.reserve(numThreads);
        m_parititonMaxTime = vector<Type_Ts>(numThreads,1);
        for (int i = 0; i < numThreads; ++i)
        {
            m_partitionIndex.push_back(numSegmentsPerThread*i);
        }
    }

    //Find starting key and it's index for every thread/partition
    m_partitionStartKey = vector<atomic<Type_Key>>(numThreads);
    m_partitionEndKey = vector<atomic<Type_Key>>(numThreads);
    for (int i = 0; i < numThreads; ++i)
    {
        int index = m_partitionIndex[i];
        if (m_ptr[index] == nullptr)
        {
            int foundPos = bitmap_closest_right_nongap(index,(i == numThreads-1)? m_numSeg-1 : m_partitionIndex[i+1]-1);
            ASSERT_MESSAGE(foundPos != -1, "foundPos is -1, entire partition contains empty segments");

            m_partitionStartKey[i] = m_ptr[foundPos]->m_currentNodeStartKey;
        }
        else
        {
            m_partitionStartKey[i] = m_ptr[index]->m_currentNodeStartKey;
        }
        m_partitionEndKey[i] = (i == numThreads-1)? m_keys.back() : m_keys[m_partitionIndex[i+1]-1];
    }

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","partition_threads");
    #endif
}

/*
Predict (Returns the thread responsible for the query)
*/
template<class Type_Key, class Type_Ts>
vector<uint32_t>  SWmeta<Type_Key,Type_Ts>::predict_thread(Type_Key key)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","predict_thread(key)");
    #endif

    if (m_numSeg == 1 || m_slope == -1)
    {
        return {1};
    }

    tuple<int,int,int> predictPosBound = find_predict_pos_bound(key);

    int startThread = 0;
    int endThread = 0;    
    for (int i = 0; i < m_partitionIndex.size(); ++i)
    {
        if (get<1>(predictPosBound) >= m_partitionIndex[i])
        {
            ++startThread; 
        }

        if (get<2>(predictPosBound) >= m_partitionIndex[i])
        {
            ++endThread; 
        }
    }

    if (startThread == endThread)
    {
        return {startThread};
    }
    else
    {
        vector<uint32_t> threadList;
        threadList.reserve(endThread - startThread + 1);
        for (int i = startThread; i <= endThread; ++i)
        {
            threadList.push_back(i);
        }
        return threadList;
    }

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","predict_thread(key)");
    #endif
}

template<class Type_Key, class Type_Ts>
vector<uint32_t>  SWmeta<Type_Key,Type_Ts>::predict_thread(Type_Key key, tuple<bool,int,int,int> & predictBound)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","predict_thread(key, predictBound for meta)");
    #endif

    if (m_numSeg == 1 || m_slope == -1)
    {
        predictBound = make_tuple(false,0,0,m_numSeg-1);
        return {1};
    }

    tuple<int,int,int> predictPosBound = find_predict_pos_bound(key);
    predictBound = make_tuple(true,get<0>(predictPosBound),get<1>(predictPosBound),get<2>(predictPosBound));

    int startThread = 0;
    int endThread = 0;    
    for (int i = 0; i < m_partitionIndex.size(); ++i)
    {
        if (get<1>(predictPosBound) >= m_partitionIndex[i])
        {
            ++startThread; 
        }

        if (get<2>(predictPosBound) >= m_partitionIndex[i])
        {
            ++endThread; 
        }
    }

    if (startThread == endThread)
    {
        return {startThread};
    }
    else
    {
        vector<uint32_t> threadList;
        threadList.reserve(endThread - startThread + 1);
        for (int i = startThread; i <= endThread; ++i)
        {
            threadList.push_back(i);
        }
        return threadList;
    }

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","predict_thread(key, predictBound for meta)");
    #endif
}

template<class Type_Key, class Type_Ts>
vector<uint32_t>  SWmeta<Type_Key,Type_Ts>::predict_thread(Type_Key key, vector<tuple<bool,int,int,int>> & predictBound)
// tuple<expSearchFlag,predictBound,predictBoundMin,predictBoundMax>
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","predict_thread(key,predictBound)");
    #endif

    if (m_numSeg == 1 || m_slope == -1)
    {
        predictBound.push_back(make_tuple(false,0,0,m_numSeg-1));
        return {1};
    }

    tuple<int,int,int> predictPosBound = find_predict_pos_bound(key);

    int startThread = 0;
    int endThread = 0;    
    for (int i = 0; i < m_partitionIndex.size(); ++i)
    {
        if (get<1>(predictPosBound) >= m_partitionIndex[i])
        {
            ++startThread; 
        }

        if (get<2>(predictPosBound) >= m_partitionIndex[i])
        {
            ++endThread; 
        }
    }

    if (startThread == endThread)
    {
        predictBound.push_back(make_tuple(true,get<0>(predictPosBound),get<1>(predictPosBound),get<2>(predictPosBound)));
        return {startThread};
    }
    else
    {
        vector<uint32_t> threadList;
        threadList.reserve(endThread - startThread + 1);
        predictBound.reserve(predictBound.size() + endThread - startThread + 1);
        for (int i = startThread; i <= endThread; ++i)
        {
            int startIndex = (i == 1)? 0 : m_partitionIndex[i-1];
            int endIndex = (i == m_partitionIndex.size())? m_numSeg-1 : m_partitionIndex[i]-1;

            bool expSearchFlag = (startIndex < get<0>(predictPosBound) && get<0>(predictPosBound) < endIndex);

            int startPredictBound = max(startIndex,get<1>(predictPosBound));
            int endPredictBound = min(endIndex,get<2>(predictPosBound)); 

            if (endPredictBound <= startPredictBound)
            {
                if (startPredictBound == startIndex)
                {
                    predictBound.push_back(make_tuple(false, get<0>(predictPosBound), startIndex, startIndex));
                }
                else if (endPredictBound == endIndex)
                {
                    predictBound.push_back(make_tuple(false, get<0>(predictPosBound), endIndex, endIndex));
                }
                else
                {
                    LOG_ERROR("SWmeta [predict_thread] : endPredictBound (%i) <= startPredictBound (%i) but not at start or end of thread \n",
                                endPredictBound, startPredictBound);
                    abort();
                }
            }
            else
            {
                bool expSearchFlag = (startIndex <= get<0>(predictPosBound) && get<0>(predictPosBound) <= endIndex);
                predictBound.push_back(make_tuple(expSearchFlag, get<0>(predictPosBound), startPredictBound, endPredictBound));
            }
            threadList.push_back(i);
        }
        return threadList;
    }

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","predict_thread(key,predictBound)");
    #endif
}

template<class Type_Key, class Type_Ts>
vector<uint32_t>  SWmeta<Type_Key,Type_Ts>::predict_thread(Type_Key lowerBound, Type_Key upperBound, vector<tuple<bool,int,int,int>> & predictBound)
// tuple<expSearchFlag,predictBound,predictBoundMin,predictBoundMax> //If predictBound = -1 (scan thread)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","predict_thread(lowerBound,upperBound,predictBound)");
    #endif

    if (m_numSeg == 1 || m_slope == -1)
    {
        predictBound.push_back(make_tuple(false,0,0,m_numSeg-1));
        return {1};
    }

    vector<uint32_t> threadList = predict_thread(lowerBound,predictBound);
    vector<uint32_t> upperBoundThreads = predict_thread(upperBound);

    if (threadList.back() < upperBoundThreads.front()) //No overlap
    {
        predictBound.reserve(predictBound.size() + upperBoundThreads.size() + upperBoundThreads.front()-threadList.back()-1);
        threadList.reserve(predictBound.size() + upperBoundThreads.size() + upperBoundThreads.front()-threadList.back()-1);
        for (int i = threadList.back()+1; i < upperBoundThreads.back(); ++i)
        {
            predictBound.push_back(make_tuple(false,-1,
                        (i == 1)? 0 : m_partitionIndex[i-1],
                        (i == m_partitionIndex.size())? m_numSeg-1 : m_partitionIndex[i]-1));
            threadList.push_back(i);
        }
        return threadList;
    }
    else //Overlap between lower and upper bounds (keep lower bound -> since scan starts from lower to upper)
    {
        predictBound.reserve(predictBound.size() + upperBoundThreads.back() - threadList.back());
        threadList.reserve(predictBound.size() + upperBoundThreads.back() - threadList.back());

        for (int i = threadList.back()+1; i <= upperBoundThreads.back(); ++i)
        {
            predictBound.push_back(make_tuple(false,-1,
                        (i == 1)? 0 : m_partitionIndex[i-1],
                        (i == m_partitionIndex.size())? m_numSeg-1 : m_partitionIndex[i]-1));
            threadList.push_back(i);
        }
    }

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","predict_thread(lowerBound,upperBound,predictBound)");
    #endif
}


/*
Predict if key is within current thread
Used when worker threads recieve queries directly from dispatcher
*/
template<class Type_Key, class Type_Ts>
bool SWmeta<Type_Key,Type_Ts>::within_thread(uint32_t threadID, Type_Key key, tuple<bool,int,int,int> & predictBound)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","within_thread(key)");
    #endif

    if (m_numSeg == 1 || m_slope == -1)
    {
        predictBound = make_tuple(false,0, 0, m_numSeg-1);
        return (threadID == 1); //Only thread 1 launched for queries
    }

    tuple<int,int,int> predictPosBound = find_predict_pos_bound(key);

    int startThread = 0;
    int endThread = 0;    
    for (int i = 0; i < m_partitionIndex.size(); ++i)
    {
        if (get<1>(predictPosBound) >= m_partitionIndex[i])
        {
            ++startThread; 
        }

        if (get<2>(predictPosBound) >= m_partitionIndex[i])
        {
            ++endThread; 
        }
    }

    if (threadID < startThread || endThread < threadID)
    {
        return false;
    }
    
    int startIndex = (threadID == 1)? 0 : m_partitionIndex[threadID-1];
    int endIndex = (threadID == m_partitionIndex.size())? m_numSeg-1 : m_partitionIndex[threadID]-1;

    int startPredictBound = max(startIndex,get<1>(predictPosBound));
    int endPredictBound = min(endIndex,get<2>(predictPosBound)); 

    if (endPredictBound <= startPredictBound)
    {
        if (startPredictBound == startIndex)
        {
            predictBound = make_tuple(false, get<0>(predictPosBound), startIndex, startIndex);
        }
        else if (endPredictBound == endIndex)
        {
            predictBound = make_tuple(false, get<0>(predictPosBound), endIndex, endIndex);
        }
        else
        {
            LOG_ERROR("SWmeta [within_thread] : endPredictBound (%i) <= startPredictBound (%i) but not at start or end of thread \n",
                            endPredictBound, startPredictBound);
            abort();
        }
    }
    else
    {
        bool expSearchFlag = (startIndex <= get<0>(predictPosBound) && get<0>(predictPosBound) <= endIndex);
        predictBound = make_tuple(expSearchFlag, get<0>(predictPosBound), startPredictBound, endPredictBound);
    }
    return true;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","within_thread");
    #endif
}

template<class Type_Key, class Type_Ts>
bool SWmeta<Type_Key,Type_Ts>::within_thread(uint32_t threadID, Type_Key lowerBound, Type_Key upperBound, tuple<bool,int,int,int> & predictBound)
//get<1>predictBound == -1 indicates scans
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","within_thread(lowerBound,upperBound)");
    #endif

    if (m_numSeg == 1 || m_slope == -1)
    {
        predictBound = make_tuple(false,0, 0, m_numSeg-1);
        return (threadID == 1); //Only thread 1 launched for queries
    }

    vector<tuple<bool,int,int,int>> predictBoundLower;
    vector<uint32_t> lowerBoundThreads = predict_thread(lowerBound,predictBoundLower);

    vector<uint32_t> upperBoundThreads = predict_thread(upperBound);

    if (lowerBoundThreads.front() <= threadID && threadID <= upperBoundThreads.back())
    {
        if (threadID <= lowerBoundThreads.back())
        {
            auto it = find(lowerBoundThreads.begin(), lowerBoundThreads.end(), threadID);
            ASSERT_MESSAGE(it != lowerBoundThreads.end(), "DALMeta [within_thread] : threadID not found in lowerBoundThreads");

            predictBound = predictBoundLower[it - lowerBoundThreads.begin()];
        }
        else
        {
            predictBound = make_tuple(false,-1,
                        (threadID == 1)? 0 : m_partitionIndex[threadID-1],
                        (threadID == m_partitionIndex.size())? m_numSeg-1 : m_partitionIndex[threadID]-1);
        }
        return true;
    }
    else
    {
        return false;
    }

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","within_thread(lowerBound,upperBound)");
    #endif
}


/*
Lookup
*/
template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::lookup(uint32_t threadID, Type_Key key, Type_Ts timestamp, tuple<bool,int,int,int> & predictBound)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","lookup");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == newKey) 
    {
        DEBUG_ENTER_FUNCTION("SWmeta","lookup");
    }
    #endif

    int count = 0;
    tuple<pswix::seg_update_type,int,Type_Key> updateSeg = make_tuple(pswix::seg_update_type::NONE,0,0);
    Type_Ts expiryTime =  ((double)timestamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): timestamp - TIME_WINDOW;
    
    unique_lock<mutex> lock(thread_lock[threadID-1]);
    lock_thread(threadID, lock);

    if (m_numSeg == 1)
    {
        count = m_ptr[0]->lookup(0,m_numSeg-1,key,expiryTime,updateSeg);
    }
    else
    {
        int foundPos;

        if (threadID <= 1 && key < m_startKey)
        {
            foundPos = 0; 
        }
        else
        {
            if (m_slope != -1)
            {
                foundPos = meta_model_search(threadID, key, predictBound);
            }
            else
            {
                foundPos = meta_non_model_search(threadID, key, predictBound);
            }

            if (foundPos == -1)
            {
                unlock_thread(threadID,lock);
                return 0;
            }
        }

        if (!bitmap_exists(foundPos))
        {
            foundPos = bitmap_closest_left_nongap(foundPos, m_partitionIndex[threadID-1]);
            if (foundPos == -1)
            {
                unlock_thread(threadID,lock);
                return 0;
            }
        }

        count = m_ptr[foundPos]->lookup(find_first_segment_in_partition(threadID), find_last_segment_in_partition(threadID),
                                        key,expiryTime,updateSeg);
    }

    if (get<0>(updateSeg) != pswix::seg_update_type::NONE)
    {
        
        vector<tuple<pswix::seg_update_type,int,Type_Key>> updateSegVector = {updateSeg};
        vector<vector<pair<Type_Key,Type_Ts>>> retrainData;
        int metaRetrainStatus = thread_dispatch_update_seg(threadID, expiryTime, updateSegVector, retrainData);

        unlock_thread(threadID, lock);
        
        if (retrainData.size() > 0)
        {
            vector<tuple<Type_Key,SWseg<Type_Key,Type_Ts>*,bool>> retrainSeg;
            for (auto & data: retrainData)
            {
                seg_retrain(threadID, data, retrainSeg);
            }

            lock_thread(threadID, lock);
            for (auto & segment: retrainSeg)
            {
                metaRetrainStatus = max(metaRetrainStatus, thread_insert(threadID, get<0>(segment), get<1>(segment), get<2>(segment)));
            }
            unlock_thread(threadID, lock);
        }

        if (metaRetrainStatus > 0 ) {meta_queue.enqueue(make_tuple(task_status::RETRAIN,0,0,nullptr));}
    }

    return count;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","lookup");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == newKey) 
    {
        DEBUG_EXIT_FUNCTION("SWmeta","lookup");
    }
    #endif
}

/*
Range Query
*/
template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::range_query(uint32_t threadID, Type_Key lowerBound, Type_Ts timestamp, Type_Key upperBound, tuple<bool,int,int,int> & predictBound)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","range_query");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == lowerBound) 
    {
        DEBUG_ENTER_FUNCTION("SWmeta","range_query");
    }
    #endif

    int count = 0;
    vector<tuple<pswix::seg_update_type,int,Type_Key>> updateSeg;
    Type_Ts expiryTime =  ((double)timestamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): timestamp - TIME_WINDOW;
    
    unique_lock<mutex> lock(thread_lock[threadID-1]);
    lock_thread(threadID, lock);

    if (m_numSeg == 1)
    {
        count = m_ptr[0]->range_search(0,m_numSeg-1,lowerBound,expiryTime,upperBound,updateSeg);
    }
    else
    {
        int foundPos;

        //Search
        if (get<1>(predictBound) != -1)
        {
            if (threadID <= 1 && lowerBound < m_startKey)
            {
                foundPos = 0; 
            }
            else
            {
                if (m_slope != -1)
                {
                    foundPos = meta_model_search(threadID, lowerBound, predictBound);
                }
                else
                {
                    foundPos = meta_non_model_search(threadID, lowerBound, predictBound);
                }
                if (foundPos == -1)
                {
                    unlock_thread(threadID,lock);
                    return 0;
                }
            }

            if (!bitmap_exists(foundPos))
            {
                foundPos = bitmap_closest_left_nongap(foundPos,(threadID == 1)? 0 : m_partitionIndex[threadID-1]);
                if (foundPos == -1)
                {
                    foundPos = bitmap_closest_right_nongap(foundPos, 
                        (threadID == m_partitionIndex.size())? m_numSeg-1:m_partitionIndex[threadID]-1);
                    if (foundPos == -1 || m_keys[foundPos] > upperBound)
                    {
                        unlock_thread(threadID,lock);
                        return 0;
                    }
                }
            }

            count = m_ptr[foundPos]->range_search(find_first_segment_in_partition(threadID), find_last_segment_in_partition(threadID),
                                        lowerBound,expiryTime,upperBound,updateSeg);
        }
        //Scan (get<1>predictBound == -1 indicates scans)
        else
        {
            foundPos = get<2>(predictBound);
            if (!bitmap_exists(foundPos))
            {
                foundPos = bitmap_closest_right_nongap(foundPos, get<3>(predictBound));
                if (foundPos == -1 || m_keys[foundPos] > upperBound)
                {
                    unlock_thread(threadID,lock);
                    return 0;
                }
            }

            count = m_ptr[foundPos]->range_scan(find_first_segment_in_partition(threadID), find_last_segment_in_partition(threadID),
                                        lowerBound,expiryTime,upperBound,updateSeg);
        }
    }

    if (updateSeg.size() > 0)
    {
        vector<vector<pair<Type_Key,Type_Ts>>> retrainData;
        int metaRetrainStatus = thread_dispatch_update_seg(threadID, expiryTime, updateSeg, retrainData);

        unlock_thread(threadID,lock);

        if (retrainData.size() > 0)
        {
            vector<tuple<Type_Key,SWseg<Type_Key,Type_Ts>*,bool>> retrainSeg;
            for (auto & data: retrainData)
            {
                seg_retrain(threadID, data, retrainSeg);
            }

            lock_thread(threadID, lock);
            for (auto & segment: retrainSeg)
            {
                metaRetrainStatus = max(metaRetrainStatus,thread_insert(threadID, get<0>(segment), get<1>(segment), get<2>(segment)));
            }
            unlock_thread(threadID, lock);
        }

        if (metaRetrainStatus > 0 ) {meta_queue.enqueue(make_tuple(task_status::RETRAIN,0,0,nullptr));}
    }

    return count;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","range_query");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == lowerBound) 
    {
        DEBUG_EXIT_FUNCTION("SWmeta","range_query");
    }
    #endif
}

/*
Insertion 
*/
template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::insert(uint32_t threadID, Type_Key key, Type_Ts timestamp, tuple<bool,int,int,int> & predictBound)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","insert");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == key) 
    {
        DEBUG_ENTER_FUNCTION("SWmeta","insert");
    }
    #endif

    vector<tuple<pswix::seg_update_type,int,Type_Key>> updateSeg;
    Type_Ts expiryTime =  ((double)timestamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): timestamp - TIME_WINDOW;
    m_parititonMaxTime[threadID-1] = timestamp;

    unique_lock<mutex> lock(thread_lock[threadID-1]);
    lock_thread(threadID, lock);

    if (m_numSeg == 1)
    {
        m_ptr[0]->insert(0,m_numSeg-1,key,timestamp,expiryTime,updateSeg);
    }
    else
    {
        int foundPos;
        if (threadID <= 1 && key < m_startKey)
        {
            foundPos = 0; 
        }
        else
        {
            if (m_slope != -1)
            {
                foundPos = meta_model_search(threadID, key, predictBound);
            }
            else
            {
                foundPos = meta_non_model_search(threadID, key, predictBound);
            }
            
            if (foundPos == -1)
            {
                unlock_thread(threadID,lock);
                return 0;
            }
        }

        if (!bitmap_exists(foundPos))
        {
            foundPos = bitmap_closest_left_nongap(foundPos,(threadID == 1)? 0 : m_partitionIndex[threadID-1]);
            if (foundPos == -1)
            {
                foundPos = bitmap_closest_right_nongap(foundPos, 
                    (threadID == m_partitionIndex.size())? m_numSeg-1:m_partitionIndex[threadID]-1);
                if (foundPos == -1)
                {
                    unlock_thread(threadID,lock);
                    return 0;
                }
            }
        }

        m_ptr[foundPos]->insert(find_first_segment_in_partition(threadID), find_last_segment_in_partition(threadID),
                                key,timestamp,expiryTime,updateSeg);
    }

    if (updateSeg.size() > 0)
    {
        vector<vector<pair<Type_Key,Type_Ts>>> retrainData;
        int metaRetrainStatus = thread_dispatch_update_seg(threadID, expiryTime, updateSeg, retrainData);

        unlock_thread(threadID,lock);

        if (retrainData.size() > 0)
        {
            vector<tuple<Type_Key,SWseg<Type_Key,Type_Ts>*,bool>> retrainSeg;
            for (auto & data: retrainData)
            {
                seg_retrain(threadID, data, retrainSeg);
            }
            
            lock_thread(threadID, lock);
            for (auto & segment: retrainSeg)
            {
                metaRetrainStatus = max(metaRetrainStatus, thread_insert(threadID, get<0>(segment), get<1>(segment), get<2>(segment)));
            }
            unlock_thread(threadID, lock);
        }

        if (metaRetrainStatus > 0 ) {meta_queue.enqueue(make_tuple(task_status::RETRAIN,0,0,nullptr));}
    }

    return 1;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","insert");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == key) 
    {
        DEBUG_EXIT_FUNCTION("SWmeta","insert");
    }
    #endif
}

/*
Search Helpers
*/
template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::meta_model_search(uint32_t threadID, Type_Key targetKey, tuple<bool,int,int,int> & predictBound)
{
    update_partition_starting_gap_key(threadID);

    //Single point
    if (get<2>(predictBound) == get<3>(predictBound))
    {
        if (targetKey < m_keys[get<2>(predictBound)] && m_partitionStartKey[threadID-1] < m_keys[get<2>(predictBound)])
        {
            return -1;
        }
        return get<2>(predictBound);
    }

    //Checking starting segment range
    if (targetKey < m_partitionStartKey[threadID-1])
    {
        return (threadID == 1)? 0 : -1;
    }

    //Check last segment with neighbouring partition
    if (targetKey >= m_keys[get<3>(predictBound)])
    {
        if (threadID == m_partitionIndex.size())
        {
            return get<3>(predictBound);
        }
        Type_Key rightNeighbourSmallestKey = m_partitionStartKey[threadID];
        return (targetKey < rightNeighbourSmallestKey)? get<3>(predictBound) : -1;
    }

    //Within range
    if (get<0>(predictBound)) //exponential search
    {
        if (get<2>(predictBound) < get<3>(predictBound))
        {
            int foundPos = get<1>(predictBound);
            foundPos = (foundPos < get<2>(predictBound))? get<2>(predictBound) : foundPos;
            foundPos = (foundPos > get<3>(predictBound))? get<3>(predictBound) : foundPos;

            if (targetKey < m_keys[foundPos])
            {
                exponential_search_left(targetKey,foundPos,foundPos-get<2>(predictBound));
            }
            else if (targetKey > m_keys[foundPos])
            {
                exponential_search_right(targetKey,foundPos,get<3>(predictBound)-foundPos);
            }
            return foundPos;
        }
        else if (get<2>(predictBound) == get<3>(predictBound))
        {
            if (targetKey < m_keys[get<1>(predictBound)])
            {
                return get<1>(predictBound)-1;
            }
            else
            {
                return get<1>(predictBound);
            }
        }
        else
        {
            return get<3>(predictBound);
        }
    }
    else //binary search
    {
        auto lowerBoundIt = lower_bound(m_keys.begin()+get<2>(predictBound),m_keys.begin()+get<3>(predictBound)+1,targetKey);
 
        if (lowerBoundIt == m_keys.begin()+get<3>(predictBound)+1 || 
                ((lowerBoundIt != m_keys.begin()+get<2>(predictBound)) && (*lowerBoundIt > targetKey)))
        {
            lowerBoundIt--;
        }

        return lowerBoundIt - m_keys.begin();
    }
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::meta_non_model_search(uint32_t threadID, Type_Key targetKey, tuple<bool,int,int,int> & searchBound)
{
    //Single point
    if (get<2>(searchBound) == get<3>(searchBound))
    {
        if (targetKey < m_keys[get<2>(searchBound) && m_partitionStartKey[threadID-1] < m_keys[get<2>(searchBound)]])
        {
            return -1;
        }
        return get<2>(searchBound);
    }

    //Checking starting segment range
    if (targetKey < m_partitionStartKey[threadID-1])
    {
        return (threadID == 1)? 0 : -1;
    }

    //Check last segment with neighbouring partition
    if (targetKey >= m_keys[get<3>(searchBound)])
    {
        if (threadID == m_partitionIndex.size())
        {
            return get<3>(searchBound);
        }
        Type_Key rightNeighbourSmallestKey = m_partitionStartKey[threadID];
        return (targetKey < rightNeighbourSmallestKey)? get<3>(searchBound) : -1;
    }

    //Within range
    auto lowerBoundIt = lower_bound(m_keys.begin()+get<2>(searchBound),m_keys.begin()+get<3>(searchBound)+1,targetKey);

    if (lowerBoundIt == m_keys.begin()+get<3>(searchBound)+1 || ((lowerBoundIt != m_keys.begin()+get<2>(searchBound)) && (*lowerBoundIt > targetKey)))
    {
        lowerBoundIt--;
    }

    return lowerBoundIt - m_keys.begin();
}

/*
Update Segments from Meta and Rendezvous Retrain within thread range
*/
template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::thread_dispatch_update_seg(uint32_t threadID, Type_Ts expiryTime, 
                                                            vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg,
                                                            vector<vector<pair<Type_Key,Type_Ts>>> & retrainData)
{
    int metaRetrainStatus = 0;
    int index;
    vector<int> retrainSegmentIndex;
    retrainSegmentIndex.reserve(updateSeg.size());
    for (auto & segment: updateSeg)
    {
        index = get<1>(segment);
        switch (get<0>(segment))
        {
            case pswix::seg_update_type::RETRAIN: //Rendezvous Retrain (flag first time)
            {
                if (bitmap_exists(m_retrainBitmap,index))
                {
                    retrainSegmentIndex.push_back(index);
                }
                else
                {
                    bitmap_set_bit(m_retrainBitmap,index);
                }
                break;
            }
            case pswix::seg_update_type::REPLACE: //Replace Segment with reinsertion
            {
                bool currentRetrainStatus = (bitmap_exists(m_retrainBitmap,index));
                SWseg<Type_Key,Type_Ts>* segPtr = m_ptr[index];

                m_ptr[index] = nullptr;
                bitmap_erase_bit(index);
                bitmap_erase_bit(m_retrainBitmap,index);
                update_keys_with_previous(threadID, index);
                --m_numSegExist;

                metaRetrainStatus = thread_insert(threadID, get<2>(segment), segPtr, currentRetrainStatus);
                break;
            }
            case pswix::seg_update_type::DELETE: //Delete Segments
            {
                delete m_ptr[index];
                m_ptr[index] = nullptr;
                bitmap_erase_bit(index);
                bitmap_erase_bit(m_retrainBitmap,index);
                update_keys_with_previous(threadID, index);

                --m_numSegExist;
                break;
            }
            default:
            {
                LOG_ERROR("SWmeta::thread_update_seg: Unknown seg_update_type");
                break;
            }
        }
    }

    //Retrain Segments and Update Meta
    retrainData.reserve(retrainSegmentIndex.size());
    for (auto & index: retrainSegmentIndex)
    {
        if (bitmap_exists(m_retrainBitmap,index))
        {
            #ifdef TUNE
            segNoRetrain++;
            #endif
            
            int startIndex, endIndex;
            tie(startIndex,endIndex) = bitmap_retrain_range(index);
            startIndex = max(startIndex,(threadID == 1)? 0 : m_partitionIndex[threadID-1]);
            endIndex = min(endIndex,(threadID == m_partitionIndex.size())? m_numSeg-1 : m_partitionIndex[threadID]-1);
            // seg_retrain(threadID, startIndex, endIndex, expiryTime);
            retrainData.push_back(seg_merge_data(threadID, startIndex, endIndex, expiryTime));

            //Delete old segments
            if (startIndex == endIndex)
            {
                delete m_ptr[index];
                m_ptr[index] = nullptr;
                bitmap_erase_bit(index);
                bitmap_erase_bit(m_retrainBitmap,index);
                --m_numSegExist;
            }
            else
            {
                for (int i = startIndex; i <= endIndex; ++i)
                {
                    if (bitmap_exists(i))
                    {
                        delete m_ptr[i];
                        m_ptr[i] = nullptr;
                        bitmap_erase_bit(i);
                        bitmap_erase_bit(m_retrainBitmap,i);
                        --m_numSegExist;
                    }
                }
            }
            update_keys_with_previous(threadID, index);
        }
    }
    update_partition_start_end_key(threadID);

    return metaRetrainStatus;
}

//Merge data
template<class Type_Key, class Type_Ts>
vector<pair<Type_Key,Type_Ts>> SWmeta<Type_Key,Type_Ts>::seg_merge_data(uint32_t threadID, int startIndex, int endIndex, Type_Ts expiryTime)
{
    vector<pair<Type_Key,Type_Ts>> data;
    int firstSegment = find_first_segment_in_partition(threadID);
    int lastSegment = find_last_segment_in_partition(threadID);

    //Retrain Alone
    if (startIndex == endIndex)
    {
        m_ptr[startIndex]->merge_data(expiryTime,data,firstSegment,lastSegment);
    }
    else //Retrain with neighbours
    {
        while(startIndex <= endIndex)
        {
            if (bitmap_exists(startIndex))
            { 
                m_ptr[startIndex]->merge_data(expiryTime,data,firstSegment,lastSegment);
            }
            ++startIndex;
        }
    }
    return data;
}

//Retrain Segments
template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::seg_retrain(uint32_t threadID, int startIndex, int endIndex, Type_Ts expiryTime)
{  
    vector<pair<Type_Key,Type_Ts>> data;
    int firstSegment = find_first_segment_in_partition(threadID);
    int lastSegment = find_last_segment_in_partition(threadID);

    //Retrain Alone
    if (startIndex == endIndex)
    {
        m_ptr[startIndex]->merge_data(expiryTime,data,firstSegment,lastSegment);
    }
    else //Retrain with neighbours
    {
        while(startIndex <= endIndex)
        {
            if (bitmap_exists(startIndex))
            { 
                m_ptr[startIndex]->merge_data(expiryTime,data,firstSegment,lastSegment);
            }
            ++startIndex;
        }
    }

    vector<tuple<int,int,double>> splitIndexSlopeVector;
    calculate_split_index_one_pass_least_sqaure(data,splitIndexSlopeVector,splitError);

    for (auto it = splitIndexSlopeVector.begin(); it != splitIndexSlopeVector.end()-1; it++)
    {
        SWseg<Type_Key,Type_Ts>* SWsegPtr = new SWseg<Type_Key,Type_Ts>(get<0>(*it), get<1>(*it) ,get<2>(*it), data);
        thread_insert(threadID, data[get<0>(*it)].first, SWsegPtr, false);
    }

    //Check last segment if it contains single point
    if (get<0>(splitIndexSlopeVector.back())  != get<1>(splitIndexSlopeVector.back()))
    {
        SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(get<0>(splitIndexSlopeVector.back()), get<1>(splitIndexSlopeVector.back()) ,get<2>(splitIndexSlopeVector.back()), data);
        thread_insert(threadID, data[get<0>(splitIndexSlopeVector.back())].first, SWsegPtr, false);

    }
    else //Single Point 
    {
        SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(data.back());
        thread_insert(threadID, data.back().first, SWsegPtr, false);
    }
}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::seg_retrain(uint32_t threadID, vector<pair<Type_Key,Type_Ts>> & data,
                                            vector<tuple<Type_Key,SWseg<Type_Key,Type_Ts>*,bool>> & retrainSeg)
{
    
    vector<tuple<int,int,double>> splitIndexSlopeVector;
    calculate_split_index_one_pass_least_sqaure(data,splitIndexSlopeVector,splitError);

    retrainSeg.reserve(retrainSeg.size() + splitIndexSlopeVector.size());

    for (auto it = splitIndexSlopeVector.begin(); it != splitIndexSlopeVector.end()-1; it++)
    {
        SWseg<Type_Key,Type_Ts>* SWsegPtr = new SWseg<Type_Key,Type_Ts>(get<0>(*it), get<1>(*it) ,get<2>(*it), data);
        retrainSeg.push_back(make_tuple(data[get<0>(*it)].first, SWsegPtr, false));
    }

    //Check last segment if it contains single point
    if (get<0>(splitIndexSlopeVector.back())  != get<1>(splitIndexSlopeVector.back()))
    {
        SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(get<0>(splitIndexSlopeVector.back()), get<1>(splitIndexSlopeVector.back()) ,get<2>(splitIndexSlopeVector.back()), data);
        retrainSeg.push_back(make_tuple(data[get<0>(splitIndexSlopeVector.back())].first, SWsegPtr, false));

    }
    else //Single Point 
    {
        SWseg<Type_Key,Type_Ts> * SWsegPtr = new SWseg<Type_Key,Type_Ts>(data.back());
        retrainSeg.push_back(make_tuple(data.back().first, SWsegPtr, false));
    }

    #ifdef TUNE
    segLengthRetrain += data.size();
    #endif
}

/*
Insert into meta by threads (attempt)
*/
template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::thread_insert(uint32_t threadID, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool currentRetrainStatus)
{
    if (m_slope != -1) //There is a model (If gap not within the range of partition, let meta insert segment)
    {
        tuple<bool,int,int,int> predictBound;
        vector<uint32_t> threadList = predict_thread(key, predictBound);

        if (threadList.size() != 1)
        {
            meta_queue.enqueue(make_tuple(task_status::INSERT,currentRetrainStatus,key,segPtr));
            return -1;
        }
        
        int insertPos = get<1>(predictBound);
        
        int metaRetrainFlag = 0;
        if (key < m_startKey)
        {
            ASSERT_MESSAGE(threadID == 1, "only thread 1 should be inserting");
            int gapPos = find_closest_gap_within_thread(threadID, 0);
            if (gapPos == -1)
            {
                meta_queue.enqueue(make_tuple(task_status::INSERT,currentRetrainStatus,key,segPtr));
                return -1;
            }
            metaRetrainFlag = insert_model(threadID, 0, gapPos, key, segPtr, false, currentRetrainStatus);
        }
        else
        {
            int predictPosMaxUnbounded = (m_rightSearchBound == 0) ? insertPos + 1 : insertPos + m_rightSearchBound;
            
            insertPos = insertPos < get<2>(predictBound) ? get<2>(predictBound) : insertPos;
            insertPos = insertPos > get<3>(predictBound) ? get<3>(predictBound) : insertPos;

            if (key < m_keys[insertPos])
            {
                exponential_search_left_insert(key, insertPos, insertPos - get<2>(predictBound));
            }
            else if (key > m_keys[insertPos])
            {
                exponential_search_right_insert(key, insertPos, get<3>(predictBound) - insertPos);
            }

            int gapPos = find_closest_gap_within_thread(threadID, insertPos);
            if (gapPos == -1)
            {
                meta_queue.enqueue(make_tuple(task_status::INSERT,currentRetrainStatus,key,segPtr));
                return -1;
            }
            else
            {
                metaRetrainFlag = insert_model(threadID,insertPos, gapPos, key, segPtr, (insertPos > predictPosMaxUnbounded), currentRetrainStatus);
            }
        }

        m_maxSearchError = min(8192,(int)ceil(0.6*m_numSeg));
        update_partition_start_end_key(threadID);

        return metaRetrainFlag;
    }
    else //m_slope == -1 (Only thread 1 is running operations except meta retrain)
    {
        ASSERT_MESSAGE(threadID == 1, "m_slope == -1, only thread 1 should be inserting");

        int insertPos = 0;
        if (m_numSeg > 1)
        {
            if (key < m_startKey)
            {
                int gapPos = find_closest_gap(insertPos);
                insert_model(threadID, 0, gapPos, key, segPtr, false, currentRetrainStatus);
            }
            else
            {
                //Binary Search
                auto lowerBoundIt = lower_bound(m_keys.begin(),m_keys.end()-1,key);
                insertPos = lowerBoundIt - m_keys.begin();

                if (*lowerBoundIt < key && bitmap_exists(insertPos))
                {
                    ++insertPos;
                }

                if (insertPos < m_numSeg)
                {
                    int gapPos = find_closest_gap(insertPos);
                    insert_model(threadID, insertPos, gapPos, key, segPtr, false, currentRetrainStatus);
                }
                else
                {
                    //Append
                    insertPos = m_numSeg;
                    m_keys.push_back(key);
                    m_ptr.push_back(segPtr);
                    ++m_numSeg;
                    ++m_numSegExist;

                    if (static_cast<int>(insertPos >> 6) == m_bitmap.size())
                    {
                        m_bitmap.push_back(0);
                        m_retrainBitmap.push_back(0);
                    }
                    bitmap_set_bit(insertPos);
                    if (currentRetrainStatus)
                    {
                        bitmap_set_bit(m_retrainBitmap,insertPos);
                    }

                    segPtr->m_parentIndex = insertPos;
                    update_segment_with_neighbours(insertPos, 0, m_numSeg-1);
                }
            }
        }
        else
        {
            insert_end(key, segPtr, currentRetrainStatus);         
        }

        m_startKey = m_keys[0];
        m_numSeg = m_keys.size();

        m_partitionStartKey[0] = m_startKey;
        m_partitionEndKey[0] = m_keys[m_numSeg-1];

        return (m_numSeg == MAX_BUFFER_SIZE)? 2 : 0;
    }
}


/*
Insert into meta by meta thread
*/
template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::meta_insert(uint32_t threadID, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool currentRetrainStatus)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWmeta","meta_insert");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == key) 
    {
        DEBUG_ENTER_FUNCTION("SWmeta","met_insert");
    }
    #endif
    
    ASSERT_MESSAGE(threadID == 0, "Only meta thread can insert into SWmeta");
    ASSERT_MESSAGE(m_slope != -1, "m_slope == -1, thread 1 can insert without informing meta thread");
    
    int metaRetrainFlag;

    tuple<bool,int,int,int> predictBound;
    vector<uint32_t> threadList = predict_thread(key, predictBound);
    
    int insertPos = get<1>(predictBound);
    int leftBoundary = (threadList[0] == 1) ? 0 :  m_partitionIndex[threadList[0]-1];
    int rightBoundary = (threadList.back() == m_partitionIndex.size()) ? m_numSeg.load() : m_partitionIndex[threadList.back()]-1;

    // Synchronization: Obtain lock for all threads in threadList
    vector<unique_lock<mutex>> locks(m_partitionIndex.size());
    
    vector<bool> threadLockFlag(m_partitionIndex.size(),false);
    for (auto & threads: threadList)
    {
        threadLockFlag[threads-1] = true;
        // thread_lock[threads-1].lock();
        locks[threads-1] = unique_lock<mutex>(thread_lock[threads-1]);
        lock_thread(threads,locks[threads-1]);
    }

    if (key < m_startKey)
    {
        ASSERT_MESSAGE(threadLockFlag[0] == true, "Thread 1 should be locked");

        int gapPos = find_closest_gap(insertPos, leftBoundary, rightBoundary);
        if (gapPos == -1) {gapPos = find_closest_gap(insertPos);}

        meta_synchroize_thread_for_insert(1, find_index_partition(gapPos), threadLockFlag, locks);
        metaRetrainFlag = insert_model(threadID, 0, gapPos, key, segPtr, false, currentRetrainStatus);
    }
    else
    {
        if (get<2>(predictBound) < get<3>(predictBound))
        {
            int predictPosMaxUnbounded = (m_rightSearchBound == 0) ? insertPos + 1 : insertPos + m_rightSearchBound;
            
            insertPos = insertPos < get<2>(predictBound) ? get<2>(predictBound) : insertPos;
            insertPos = insertPos > get<3>(predictBound) ? get<3>(predictBound) : insertPos;

            if (key < m_keys[insertPos])
            {
                exponential_search_left_insert(key, insertPos, insertPos - get<2>(predictBound));
            }
            else if (key > m_keys[insertPos])
            {
                exponential_search_right_insert(key, insertPos, get<3>(predictBound) - insertPos);
            }
            
            if (insertPos < m_numSeg)
            {
                int gapPos = find_closest_gap(insertPos, leftBoundary, rightBoundary);
                if (gapPos == -1) {gapPos = find_closest_gap(insertPos);}

                if (gapPos < insertPos)
                {
                    meta_synchroize_thread_for_insert(find_index_partition(gapPos), find_index_partition(insertPos), threadLockFlag, locks);
                }
                else
                {
                    meta_synchroize_thread_for_insert(find_index_partition(insertPos), find_index_partition(gapPos), threadLockFlag, locks);
                }

                metaRetrainFlag = insert_model(threadID, insertPos, gapPos, key, segPtr, (insertPos > predictPosMaxUnbounded), currentRetrainStatus);
            }
            else
            {
                //Append (only last segment need lock)
                meta_synchroize_thread_for_insert(m_partitionIndex.size(), m_partitionIndex.size(), threadLockFlag, locks);

                insertPos = m_numSeg;
                m_keys.push_back(key);
                m_ptr.push_back(segPtr);
                ++m_numSeg;
                ++m_numSegExist;

                if (static_cast<int>(insertPos >> 6) == m_bitmap.size())
                {
                    m_bitmap.push_back(0);
                    m_retrainBitmap.push_back(0);
                }
                bitmap_set_bit(insertPos);
                if (currentRetrainStatus)
                {
                    bitmap_set_bit(m_retrainBitmap,insertPos);
                }

                segPtr->m_parentIndex = insertPos;
                update_segment_with_neighbours(insertPos, m_partitionIndex.back(), m_numSeg-1);

                if (predictPosMaxUnbounded < insertPos)
                {
                    ++m_rightSearchBound;
                }

                if (m_leftSearchBound + m_rightSearchBound > m_maxSearchError || (double)m_numSegExist / m_numSeg > 0.8)
                {
                    metaRetrainFlag = ((double)m_numSegExist / m_numSeg < 0.5) ? 2 : 1;
                }
                metaRetrainFlag = 0;
            }
        }
        else if (get<2>(predictBound) == get<3>(predictBound))
        {
            //Insert to End (only last segment need lock)
            meta_synchroize_thread_for_insert(m_partitionIndex.size(), m_partitionIndex.size(), threadLockFlag, locks);
            metaRetrainFlag = insert_end(key, segPtr, currentRetrainStatus);
        }
        else
        {
            //Append (only last segment need lock)
            meta_synchroize_thread_for_insert(m_partitionIndex.size(), m_partitionIndex.size(), threadLockFlag, locks);
            metaRetrainFlag = insert_append(insertPos, key, segPtr, currentRetrainStatus);
        }
    }

    m_maxSearchError = min(8192,(int)ceil(0.6*m_numSeg));
    
    // Synchronization:  unlock threads when completed
    for (uint32_t thread = 0; thread < m_partitionIndex.size(); ++thread)
    {
        if (threadLockFlag[thread]) 
        { 
            update_partition_start_end_key(thread+1);
            unlock_thread(thread+1,locks[thread]);
            threadLockFlag[thread] = false;
        }
    }

    return metaRetrainFlag;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWmeta","meta_insert");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == key) 
    {
        DEBUG_EXIT_FUNCTION("SWmeta","met_insert");
    }
    #endif
}

/*
Insertion Helpers
*/
template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::insert_model(uint32_t threadID, int insertionPos, int gapPos, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool addErrorFlag, bool currentRetrainStatus)
{
    //Find Boundaries
    int leftBoundary;
    int rightBoundary;
    if (threadID != 0)
    {
        leftBoundary = (threadID == 1)? 0 : m_partitionIndex[threadID-1];
        rightBoundary = (threadID == m_partitionIndex.size())? m_numSeg.load() : m_partitionIndex[threadID]-1;
    }
    else
    {
        uint32_t threadIDTemp;
        if (insertionPos < gapPos)
        {
            threadIDTemp = find_index_partition(insertionPos);
            leftBoundary = (threadIDTemp == 1) ? 0 :  m_partitionIndex[threadIDTemp-1];
            threadIDTemp = find_index_partition(gapPos);
            rightBoundary = (threadIDTemp == m_partitionIndex.size()) ? m_numSeg.load() : m_partitionIndex[threadIDTemp]-1;
        }
        else
        {
            threadIDTemp = find_index_partition(gapPos);
            leftBoundary = (threadIDTemp == 1) ? 0 :  m_partitionIndex[threadIDTemp-1];
            threadIDTemp = find_index_partition(insertionPos);
            rightBoundary = (threadIDTemp == m_partitionIndex.size()) ? m_numSeg.load() : m_partitionIndex[threadIDTemp]-1;
        }
    }
    
    //Insertion position is not a gap
    if (insertionPos != gapPos)
    {
        if (gapPos != m_numSeg)
        {
            if (insertionPos < gapPos) //Right Shift (Move)
            {
                vector<Type_Key> tempKey(m_keys.begin()+insertionPos,m_keys.begin()+gapPos); //May cause error when simulatenous insert (change to more traditional way if that is the case)
                move(tempKey.begin(),tempKey.end(),m_keys.begin()+insertionPos+1);
                m_keys[insertionPos] = key;

                for (auto it = m_ptr.begin() + insertionPos; it != m_ptr.begin() + gapPos; ++it)
                {
                    if ((*it) != nullptr)
                    {
                        ++((*it)->m_parentIndex);
                    }
                }
                vector<SWseg<Type_Key,Type_Ts>*> tempPtr(m_ptr.begin()+insertionPos,m_ptr.begin()+gapPos);
                move(tempPtr.begin(),tempPtr.end(),m_ptr.begin()+insertionPos+1);
                m_ptr[insertionPos] = segPtr;

                bitmap_set_bit(insertionPos);
                bitmap_move_bit_back(insertionPos,gapPos);
                bitmap_move_bit_back(m_retrainBitmap,insertionPos,gapPos);

                if (currentRetrainStatus)
                {
                    bitmap_set_bit(m_retrainBitmap,insertionPos);
                }
                else
                {
                    bitmap_erase_bit(m_retrainBitmap,insertionPos);
                }

                segPtr->m_parentIndex = insertionPos;
                update_segment_with_neighbours(insertionPos, leftBoundary, rightBoundary);
                
                ++m_numSegExist;
                ++m_rightSearchBound;
            }
            else //Left Shift (Move)
            {
                --insertionPos;
                vector<Type_Key> tempKey(m_keys.begin()+gapPos+1,m_keys.begin()+insertionPos+1);
                move(tempKey.begin(),tempKey.end(),m_keys.begin()+gapPos);
                m_keys[insertionPos] = key;

                for (auto it = m_ptr.begin() + insertionPos; it != m_ptr.begin() + gapPos; --it)
                {
                    if ((*it) != nullptr)
                    {
                        --((*it)->m_parentIndex);
                    }
                }

                vector<SWseg<Type_Key,Type_Ts>*> tempPtr(m_ptr.begin()+gapPos+1,m_ptr.begin()+insertionPos+1);
                move(tempPtr.begin(),tempPtr.end(),m_ptr.begin()+gapPos);
                m_ptr[insertionPos] = segPtr;

                bitmap_set_bit(insertionPos);
                bitmap_move_bit_front(insertionPos,gapPos);
                bitmap_move_bit_front(m_retrainBitmap,insertionPos,gapPos);

                if (currentRetrainStatus)
                {
                    bitmap_set_bit(m_retrainBitmap,insertionPos);
                }
                else
                {
                    bitmap_erase_bit(m_retrainBitmap,insertionPos);
                }

                segPtr->m_parentIndex = insertionPos;
                update_segment_with_neighbours(insertionPos, leftBoundary, rightBoundary);

                ++m_numSegExist;
                ++m_leftSearchBound;
            }
        }
        else //Right Shift (Insert)
        {
            m_keys.insert(m_keys.begin()+insertionPos,key);

            for (auto it = m_ptr.begin() + insertionPos; it != m_ptr.end(); it++)
            {
                if ((*it) != nullptr)
                {
                    ++((*it)->m_parentIndex);
                }
            }
            m_ptr.insert(m_ptr.begin()+insertionPos,segPtr);

            bitmap_set_bit(insertionPos);
            bitmap_move_bit_back(insertionPos,m_numSeg);
            bitmap_move_bit_back(m_retrainBitmap,insertionPos,m_numSeg);

            if (currentRetrainStatus)
            {
                bitmap_set_bit(m_retrainBitmap,insertionPos);
            }
            else
            {
                bitmap_erase_bit(m_retrainBitmap,insertionPos);
            }

            segPtr->m_parentIndex = insertionPos;
            update_segment_with_neighbours(insertionPos, leftBoundary, rightBoundary);
                
            ++m_numSeg;
            ++m_numSegExist;
            ++m_rightSearchBound;
        }
    }
    else
    {
        m_keys[insertionPos] = key;
        m_ptr[insertionPos] = segPtr;
        segPtr->m_parentIndex = insertionPos;
        bitmap_set_bit(insertionPos);
        if (currentRetrainStatus) {bitmap_set_bit(m_retrainBitmap, insertionPos);}
        ++ m_numSegExist;
        
        for (int i = insertionPos+1; i <= rightBoundary; ++i)
        {
            if (m_keys[i] >= key)
            {
                break;
            }
            
            if (bitmap_exists(i))
            {
                LOG_ERROR("Keys are not sorted");
            }
            else
            {
                m_keys[i] = key;
            }
        }
    }

    if (addErrorFlag) { ++m_rightSearchBound;}
    if (m_leftSearchBound + m_rightSearchBound > m_maxSearchError || (double)m_numSegExist / m_numSeg > 0.8)
    {
        return ((double)m_numSegExist / m_numSeg < 0.5) ? 2 : 1;
    }
    return 0;
}

template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::insert_end(Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool currentRetrainStatus)
{
     if (bitmap_exists(m_numSeg-1))
    {
        if (static_cast<int>(m_numSeg >> 6) == m_bitmap.size())
        {
            m_bitmap.push_back(0);
            m_retrainBitmap.push_back(0);
        }

        if (key < m_keys.back()) //Insert into numSeg-1
        {
            //Insert and increment m_rightSearchBound
            bitmap_move_bit_back(m_numSeg-1,m_numSeg);
            bitmap_set_bit(m_numSeg-1);
            bitmap_move_bit_back(m_retrainBitmap,m_numSeg-1,m_numSeg);
            if (currentRetrainStatus)
            {
                bitmap_set_bit(m_retrainBitmap,m_numSeg-1);
            }
            else
            {
                bitmap_erase_bit(m_retrainBitmap,m_numSeg-1);
            }

            m_keys.insert(m_keys.end()-1,key);
            ++m_numSeg;

            ++(m_ptr.back()->m_parentIndex);
            m_ptr.insert(m_ptr.end()-1,segPtr);

            segPtr->m_parentIndex = m_numSeg-2;
            update_segment_with_neighbours(m_numSeg-2, m_partitionIndex.back(), m_numSeg-1);

            ++m_numSegExist;
            ++m_rightSearchBound;

            if (m_leftSearchBound + m_rightSearchBound > m_maxSearchError || (double)m_numSegExist / m_numSeg > 0.8)
            {
                return ((double)m_numSegExist / m_numSeg < 0.5) ? 2 : 1;
            }
            return 0;
        }
        else //Insert into numSeg
        {
            bitmap_set_bit(m_numSeg);
            if (currentRetrainStatus)
            {
                bitmap_set_bit(m_retrainBitmap,m_numSeg);
            }

            m_keys.push_back(key);
            m_ptr.push_back(segPtr);
            ++m_numSeg;

            segPtr->m_parentIndex = m_numSeg-1;
            update_segment_with_neighbours(m_numSeg-1, m_partitionIndex.back(), m_numSeg-1);
            ++m_numSegExist;
        }
    }
    else //Replace numSeg
    {
        m_keys.back() = key;
        m_ptr.back() = segPtr;
        bitmap_set_bit(m_numSeg-1);
        if (currentRetrainStatus)
        {
            bitmap_set_bit(m_retrainBitmap,m_numSeg-1);
        }
        segPtr->m_parentIndex = m_numSeg-1;
        update_segment_with_neighbours(m_numSeg-1, m_partitionIndex.back(), m_numSeg-1);
        ++m_numSegExist;
    }

    return 0;
}

template<class Type_Key, class Type_Ts>
int SWmeta<Type_Key,Type_Ts>::insert_append(int insertionPos, Type_Key key, SWseg<Type_Key,Type_Ts>* segPtr, bool currentRetrainStatus)
{
    Type_Key lastKey = m_keys.back();
    for (int index = m_numSeg; index <= insertionPos; ++index)
    {
        m_keys.push_back(lastKey);
        m_ptr.push_back(nullptr);
        ++m_numSeg;
    }
    m_keys.back() = key;
    m_ptr.back() = segPtr;
   
    int bitmapExpandTimes = static_cast<int>(insertionPos >> 6) - (m_bitmap.size()-1);
    for (int i = 0; i < bitmapExpandTimes; i++)
    {
        m_bitmap.push_back(0);
        m_retrainBitmap.push_back(0);
    }
    bitmap_set_bit(insertionPos);
    if (currentRetrainStatus)
    {
        bitmap_set_bit(m_retrainBitmap,insertionPos);
    }

    segPtr->m_parentIndex = insertionPos;
    update_segment_with_neighbours(insertionPos, m_partitionIndex.back(), insertionPos);
    ++m_numSegExist;

    if (((double)m_numSegExist) / (insertionPos+1) < 0.5)
    {
        return 2;
    }
    else if ((double)m_numSegExist / (insertionPos+1) > 0.8)
    {
        return 1;
    }
    else
    {
        return 0;
    }
}

/*
Meta Retrain (Called by meta thread)
*/
template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::meta_retrain()
{
    //Check again to make sure we need to retrain
    int retrainMethod;
    if (m_slope != -1)
    {
        if (m_leftSearchBound + m_rightSearchBound > m_maxSearchError || (double)m_numSegExist / m_numSeg > 0.8)
        {
            retrainMethod = ((double)m_numSegExist / m_numSeg < 0.5) ? 2 : 1;
        }
        else
        {
            return;
        }
    }
    else
    {
        if (m_numSeg != MAX_BUFFER_SIZE)
        {
            return;
        }
        retrainMethod = 2;
    }

    //Retrain Model without lock
    vector<Type_Key> keys = m_keys;
    vector<uint64_t> bitmap = m_bitmap;
    int numSeg = m_numSeg;
    
    double slopeTemp = m_slope;
    if (retrainMethod == 1) //Extend
    {
        slopeTemp *= 1.05;
    }
    else //Retrain
    {
        int firstIndex;
        Type_Key firstKey;
        double nomalizedKey;
        double sumKey = 0, sumIndex = 0, sumKeyIndex = 0, sumKeySquared = 0;
        int cnt = 0;
        for (int i = 0; i < numSeg; ++i)
        {
            if (bitmap_exists(bitmap,i))
            {               
                if (!cnt)
                {
                    firstIndex = keys[i];
                    firstIndex = i;
                }
                nomalizedKey = (double)keys[i] - firstKey;
                sumKey += nomalizedKey;
                sumIndex += cnt;
                sumKeyIndex += nomalizedKey * cnt;
                sumKeySquared += (double)pow(nomalizedKey,2);
                ++cnt;
            }
        }
        if (cnt == 1)
        {
            slopeTemp = -1;
        }
        else
        {
            slopeTemp =  (sumKeyIndex - sumKey *(sumIndex/cnt))/(sumKeySquared - sumKey*(sumKey/cnt)) * 1.05;
        }
    }
    keys.clear();
    bitmap.clear();

    vector<Type_Key> tempKey;
    vector<SWseg<Type_Key,Type_Ts>*> tempPtr;
    vector<uint64_t> tempBitmap;
    vector<uint64_t> tempRetrainBitmap;

    //Lock all threads
    vector<unique_lock<mutex>> locks(m_partitionIndex.size());
    for (uint32_t threadID = 0; threadID < m_partitionIndex.size(); ++threadID)
    {
        locks[threadID] = unique_lock<mutex>(thread_lock[threadID]);
        lock_thread(threadID+1,locks[threadID]);
    }

    Type_Ts maxTimeStamp = *max_element(m_parititonMaxTime.begin(), m_parititonMaxTime.end());
    Type_Ts expiryTime =  ((double)maxTimeStamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): maxTimeStamp - TIME_WINDOW;

    //Load Data with current version of m_keys and m_ptr
    int startIndex = (bitmap_exists(0))? 0 : bitmap_closest_right_nongap(0, m_numSeg-1);
    m_startKey = m_keys[startIndex];

    tempKey.reserve(m_numSeg);
    tempPtr.reserve(m_numSeg);

    if (slopeTemp != -1)
    {     
        m_slope = slopeTemp;
        
        int currentIndex = startIndex;
        int currentInsertionPos = 0;
        int lastNonGapIndex = 0;
        int predictedPos;
        m_numSegExist = 0;
        m_rightSearchBound = 0;
        m_leftSearchBound = 0;

        while (currentIndex < m_numSeg)
        {
            if (bitmap_exists(currentIndex))
            {
                if (m_ptr[currentIndex]->m_maxTimeStamp >= expiryTime) //Segment has not expired
                {
                    predictedPos = static_cast<int>(floor(m_slope * ((double)m_keys[currentIndex] - (double)m_startKey)));
                    
                    if (static_cast<int>(currentInsertionPos >> 6) == tempBitmap.size())
                    {
                        tempBitmap.push_back(0);
                        tempRetrainBitmap.push_back(0);
                    }

                    if (predictedPos == currentInsertionPos)
                    {
                        m_ptr[currentIndex]->m_parentIndex = currentInsertionPos;

                        tempKey.push_back(m_keys[currentIndex]);
                        tempPtr.push_back(m_ptr[currentIndex]);
                        if (lastNonGapIndex > 0)
                        {
                            tempPtr[lastNonGapIndex]->m_leftSibling = tempPtr.back();
                            tempPtr.back()->m_rightSibling = tempPtr[lastNonGapIndex];
                        }
                        bitmap_set_bit(tempBitmap, currentInsertionPos);

                        if (bitmap_exists(m_retrainBitmap,currentIndex))
                        {
                            bitmap_set_bit(tempRetrainBitmap,currentInsertionPos);
                        }

                        ++m_numSegExist;
                        ++currentIndex;
                        lastNonGapIndex = currentInsertionPos;
                    }
                    else if (predictedPos < currentInsertionPos)
                    {
                        m_ptr[currentIndex]->m_parentIndex = currentInsertionPos;

                        tempKey.push_back(m_keys[currentIndex]);
                        tempPtr.push_back(m_ptr[currentIndex]);
                        if (lastNonGapIndex > 0)
                        {
                            tempPtr[lastNonGapIndex]->m_leftSibling = tempPtr.back();
                            tempPtr.back()->m_rightSibling = tempPtr[lastNonGapIndex];
                        }
                        bitmap_set_bit(tempBitmap, currentInsertionPos);

                        if (bitmap_exists(m_retrainBitmap,currentIndex))
                        {
                            bitmap_set_bit(tempRetrainBitmap,currentInsertionPos);
                        }

                        m_rightSearchBound = (currentInsertionPos - predictedPos) > m_rightSearchBound.load() 
                                            ? (currentInsertionPos - predictedPos) : m_rightSearchBound.load();
                        
                        ++m_numSegExist;
                        +currentIndex;
                        lastNonGapIndex = currentInsertionPos;
                    }
                    else
                    {
                        tempKey.push_back((tempKey.size() == 0) ? numeric_limits<Type_Key>::min() : tempKey.back());
                        tempPtr.push_back(nullptr);
                    }
                    ++currentInsertionPos;
                }
                else //Segment Expired update neighbours
                {
                    delete m_ptr[currentIndex];
                    m_ptr[currentIndex] = nullptr;
                    ++currentIndex;
                }
            }
            else
            {
                ++currentIndex;
            }
        }
        tempPtr.front()->m_leftSibling = nullptr;
        tempPtr[lastNonGapIndex]->m_rightSibling = nullptr;

        m_rightSearchBound += 1;
        m_maxSearchError = min(8192,(int)ceil(0.6*tempKey.size()));
    }
    else //No Model load all keys
    {
        ASSERT_MESSAGE(m_numSegExist < MAX_BUFFER_SIZE, "retrain m_slope = -1, but actual the number of keys is > MAX_BUFFER_SIZE");
        
        int currentIndex = startIndex;
        int currentInsertionPos = 0;

        if (m_numSegExist > 1)
        {
            while (currentIndex < m_numSeg)
            {
                if (bitmap_exists(currentIndex))
                {
                    if (m_ptr[currentIndex]->m_maxTimeStamp >= expiryTime) //Segment has not expired
                    {
                        if (static_cast<int>(currentInsertionPos >> 6) == tempBitmap.size())
                        {
                            tempBitmap.push_back(0);
                            tempRetrainBitmap.push_back(0);
                        }

                        m_ptr[currentIndex]->m_parentIndex = currentInsertionPos;

                        tempKey.push_back(m_keys[currentIndex]);
                        tempPtr.push_back(m_ptr[currentIndex]);

                        if (currentInsertionPos > 0)
                        {
                            tempPtr[currentInsertionPos-1]->m_leftSibling = tempPtr.back();
                            tempPtr.back()->m_rightSibling = tempPtr[currentInsertionPos-1];
                        }

                        bitmap_set_bit(tempBitmap, currentInsertionPos);

                        if (bitmap_exists(m_retrainBitmap,currentIndex))
                        {
                            bitmap_set_bit(tempRetrainBitmap,currentInsertionPos);
                        }

                        ++m_numSegExist;
                        ++currentIndex;
                        ++currentInsertionPos;
                    }
                    else
                    {                       
                        delete m_ptr[currentIndex]; //TODO update neighbours
                        m_ptr[currentIndex] = nullptr;
                        ++currentIndex;
                    }
                }
                else
                {
                    ++currentIndex;
                }
            }
            tempPtr.front()->m_leftSibling = nullptr;
            tempPtr.back()->m_rightSibling = nullptr;
        }
        else
        {
            int bitmapSize = ceil(MAX_BUFFER_SIZE/64.);
            vector<uint64_t> temp(bitmapSize,0);
            
            m_numSeg = 1;
            m_numSegExist = 1;
            m_bitmap = temp;
            m_retrainBitmap = temp;
            bitmap_set_bit(0);

            if (bitmap_exists(m_retrainBitmap,startIndex))
            {
                bitmap_set_bit(tempRetrainBitmap,0);
            }
            
            m_ptr[startIndex]->m_parentIndex = 0;
            m_ptr[startIndex]->m_leftSibling = nullptr;
            m_ptr[startIndex]->m_rightSibling = nullptr;

            tempKey.push_back(m_keys[startIndex]);
            tempPtr.push_back(m_ptr[startIndex]);
        }

        m_slope = -1;
        m_rightSearchBound = 0;
        m_leftSearchBound = 0;
    }

    m_keys = tempKey;
    m_ptr = tempPtr;
    m_bitmap = tempBitmap;
    m_retrainBitmap = tempRetrainBitmap;
    m_numSeg = m_keys.size();
    m_parititonMaxTime = vector<Type_Ts>(m_partitionIndex.size(),maxTimeStamp);
    partition_threads(m_partitionIndex.size());

    //Unlock all threads
    for (uint32_t threadID = 0; threadID < m_partitionIndex.size(); ++threadID)
    {
        update_partition_start_end_key(threadID+1);
        unlock_thread(threadID+1,locks[threadID]);
    }

}

/*
Util functions
*/
template<class Type_Key, class Type_Ts>
inline tuple<int,int,int> SWmeta<Type_Key,Type_Ts>::find_predict_pos_bound(Type_Key targetKey)
{
    int predictPos = static_cast<int>(floor(m_slope * ((double)targetKey - (double)m_startKey)));
    int predictPosMin = predictPos - m_leftSearchBound;
    int predictPosMax = (m_rightSearchBound == 0) ? predictPos + 1 : predictPos + m_rightSearchBound;

    return (make_tuple(predictPos, predictPosMin < 0 ? 0 : predictPosMin, predictPosMax > m_numSeg-1 ? m_numSeg-1: predictPosMax));
}


template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::exponential_search_right(Type_Key targetKey, int & foundPos, int maxSearchBound)
{
    #ifdef TUNE
    int predictPos = foundPos;
    #endif

    int index = 1;
    while (index <= maxSearchBound && m_keys[foundPos + index] < targetKey)
    {
        index *= 2;
    }

    auto startIt = m_keys.begin() + (foundPos + static_cast<int>(index/2));
    auto endIt = m_keys.begin() + (foundPos + min(index,maxSearchBound));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey);

    if (lowerBoundIt == m_keys.end() || ((lowerBoundIt != m_keys.begin()) && (*lowerBoundIt > targetKey)))
    {
        lowerBoundIt--;
    }

    foundPos = lowerBoundIt - m_keys.begin();

    #ifdef TUNE
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += foundPos - predictPos;
    #endif
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::exponential_search_right_insert(Type_Key targetKey, int & foundPos, int maxSearchBound)
{
    #ifdef TUNE
    int predictPos = foundPos;
    #endif

    int index = 1;
    while (index <= maxSearchBound && m_keys[foundPos + index] < targetKey)
    {
        index *= 2;
    }

    auto startIt = m_keys.begin() + (foundPos + static_cast<int>(index/2));
    auto endIt = m_keys.begin() + (foundPos + min(index,maxSearchBound));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey);

    foundPos = lowerBoundIt - m_keys.begin();

    if (*lowerBoundIt < targetKey && bitmap_exists(foundPos))
    {
        foundPos++;
    }

    #ifdef TUNE
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += foundPos - predictPos;
    #endif
}


template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::exponential_search_left(Type_Key targetKey, int & foundPos, int maxSearchBound)
{
    #ifdef TUNE
    int predictPos = foundPos;
    #endif

    int index = 1;
    while (index <= maxSearchBound && m_keys[foundPos - index] >= targetKey)
    {
        index *= 2;
    }

    auto startIt = m_keys.begin() + (foundPos - min(index,maxSearchBound));
    auto endIt = m_keys.begin() + (foundPos - static_cast<int>(index/2));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey);
    
    if (lowerBoundIt == m_keys.end() || ((lowerBoundIt != m_keys.begin()) && (*lowerBoundIt > targetKey)))
    {
        lowerBoundIt--;
    }

    foundPos = lowerBoundIt - m_keys.begin();

    #ifdef TUNE
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += predictPos - foundPos;
    #endif
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::exponential_search_left_insert(Type_Key targetKey, int & foundPos, int maxSearchBound)
{
    #ifdef TUNE
    int predictPos = foundPos;
    #endif

    int index = 1;
    while (index <= maxSearchBound && m_keys[foundPos - index] >= targetKey)
    {
        index *= 2;
    }

    auto startIt = m_keys.begin() + (foundPos - min(index,maxSearchBound));
    auto endIt = m_keys.begin() + (foundPos - static_cast<int>(index/2));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey);
    
    foundPos = lowerBoundIt - m_keys.begin();

    if (*lowerBoundIt < targetKey && bitmap_exists(foundPos))
    {
        foundPos++;
    }

    #ifdef TUNE
    rootNoExponentialSearch++;
    rootLengthExponentialSearch += predictPos - foundPos;
    #endif
}
template<class Type_Key, class Type_Ts>
inline uint32_t SWmeta<Type_Key,Type_Ts>::find_index_partition(int index)
{
    uint32_t threadID = 0;
    for (int i = 0; i < m_partitionIndex.size(); ++i)
    {
        if (index >= m_partitionIndex[i])
        {
            ++threadID; 
        }
    }
    return threadID;
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::find_first_segment_in_partition(uint32_t threadID)
{
    int firstSegmentIndex = (threadID == 1)? 0 : m_partitionIndex[threadID-1];

    if (!bitmap_exists(firstSegmentIndex))
    {
        firstSegmentIndex = bitmap_closest_right_nongap(firstSegmentIndex,
            (threadID == m_partitionIndex.size())? m_numSeg-1:m_partitionIndex[threadID]-1);
        
        if (firstSegmentIndex == -1) return -1;
    }
    return firstSegmentIndex;
}   

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::find_last_segment_in_partition(uint32_t threadID)
{
    int lastSegmentIndex= (threadID == m_partitionIndex.size())? m_numSeg-1 : m_partitionIndex[threadID]-1;

    if (!bitmap_exists(lastSegmentIndex))
    {
        lastSegmentIndex = bitmap_closest_left_nongap(lastSegmentIndex,
            (threadID == 1)? 0 : m_partitionIndex[threadID-1]);
        
        if (lastSegmentIndex == -1) return -1;
    }
    return lastSegmentIndex;
}


template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::find_closest_gap(int index)
{
    if (bitmap_exists(index))
    {
        int gapPos = bitmap_closest_gap(index);

        if (gapPos == -1)
        {
            LOG_ERROR("No gap found in bitmap");
            abort();
        }
        return gapPos;
    }
    return index;
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::find_closest_gap(int index, int leftBoundary, int rightBoundary)
{
    if (bitmap_exists(index))
    {
        int gapPos = bitmap_closest_gap(index, leftBoundary, rightBoundary);

        return (gapPos == -1)? -1 : gapPos;
    }
    return index;
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::find_closest_gap_within_thread(uint32_t threadID, int index)
{
    if (bitmap_exists(index))
    {
        int gapPos = bitmap_closest_gap(index,(threadID == 1)? 0 : m_partitionIndex[threadID-1],
            (threadID == m_partitionIndex.size())? m_numSeg.load():m_partitionIndex[threadID]-1);
        //Last partition allow gapPos == m_numSeg;

        return (gapPos == -1)? -1 : gapPos;
    }
    return index;
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::meta_synchroize_thread_for_insert(uint32_t startThreadID, uint32_t endThreadID, vector<bool> & threadLockStatus, vector<unique_lock<mutex>> & locks)
{
    for (uint32_t threadID = 0; threadID < m_partitionIndex.size(); ++threadID)
    {
        if (startThreadID <= threadID+1 && threadID+1 <= endThreadID)
        {
            if (!threadLockStatus[threadID]) 
            {     
                threadLockStatus[threadID] = true;
                locks[threadID] = unique_lock<mutex>(thread_lock[threadID]);
                lock_thread(threadID+1,locks[threadID]);
            }
        }
        else
        {
            if (threadLockStatus[threadID]) 
            { 
                update_partition_start_end_key(threadID+1);
                unlock_thread(threadID+1,locks[threadID]);
                threadLockStatus[threadID] = false;
            }
        }
    }
}

template<class Type_Key, class Type_Ts>
inline pair<int,int> SWmeta<Type_Key,Type_Ts>::find_neighbours(int index, int leftBoundary, int rightBoundary)
{
    int leftNeighbour = -1;
    if (leftBoundary < index)
    {
        leftNeighbour = (bitmap_exists(index-1)) ? index-1: bitmap_closest_left_nongap(index-1,leftBoundary);
    }

    int rightNeighbour = -1;
    if (index < rightBoundary)
    {
        rightNeighbour = (bitmap_exists(index+1)) ? index+1: bitmap_closest_right_nongap(index+1,rightBoundary);
    }

    return make_pair(leftNeighbour,rightNeighbour);
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::update_segment_with_neighbours(int index, int leftBoundary, int rightBoundary)
{
    ASSERT_MESSAGE(bitmap_exists(index), "Segment does not exist in bitmap");

    int leftNeighbour = -1;
    if (leftBoundary < index)
    {
        leftNeighbour = (bitmap_exists(index-1)) ? index-1: bitmap_closest_left_nongap(index-1,leftBoundary);
    }

    if (leftNeighbour != -1)
    {
        m_ptr[index]->m_leftSibling = m_ptr[leftNeighbour];
        m_ptr[leftNeighbour]->m_rightSibling = m_ptr[index];
    }

    int rightNeighbour = -1;
    if (index < rightBoundary)
    {
        rightNeighbour = (bitmap_exists(index+1)) ? index+1: bitmap_closest_right_nongap(index+1,rightBoundary);
    }

    if (rightNeighbour != -1)
    {
        m_ptr[index]->m_rightSibling = m_ptr[rightNeighbour];
        m_ptr[rightNeighbour]->m_leftSibling = m_ptr[index];
    }

}


template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::update_keys_with_previous(uint32_t threadID, int index)
{
    if (index) // Do not need to replace current key with previous key if it is the first segment
    {
        int startIndex = (threadID == 1)? 0 : find_first_segment_in_partition(threadID);
        int endIndex = (threadID == m_partitionIndex.size())? m_numSeg-1 : m_partitionIndex[threadID]-1;
        
        Type_Key previousKey = (index == startIndex)? m_partitionEndKey[threadID-2].load(): m_keys[index-1];
        m_keys[index] = previousKey;
        ++index;
        
        while (!bitmap_exists(index) && index <= endIndex)
        {
            m_keys[index] = previousKey;
            ++index;
        }

        if (index == endIndex + 1)
        {
            m_partitionEndKey[threadID-1] = previousKey;
        }
    }
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::update_partition_start_end_key(uint32_t threadID)
{
    //Starting Key
    int startExistIndex = find_first_segment_in_partition(threadID);
    ASSERT_MESSAGE(startExistIndex != -1,"Thread contains all expired segments");
    m_partitionStartKey[threadID-1] = m_ptr[startExistIndex]->m_currentNodeStartKey;
    //End Key
    m_partitionEndKey[threadID-1] = m_keys[(threadID == m_partitionIndex.size())? m_numSeg-1 : m_partitionIndex[threadID]-1];
}


template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::update_partition_starting_gap_key(uint32_t threadID)
{    
    int startIndex = (threadID == 1)? 0 : m_partitionIndex[threadID-1];
    int startExistIndex = find_first_segment_in_partition(threadID);

    if (startIndex != startExistIndex)
    {
        for (int index = startIndex; index < startExistIndex; ++index)
        {    
            if (m_keys[index] >= m_partitionEndKey[threadID-2])
            {
                break;
            }
            m_keys[index] = m_partitionEndKey[threadID-2];
        }
    }
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::lock_thread(uint32_t threadID, unique_lock<mutex> & lock)
{
    while (thread_occupied[threadID-1])
    {
        thread_cv[threadID-1].wait(lock);
    }
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::unlock_thread(uint32_t threadID, unique_lock<mutex> & lock)
{
    thread_occupied[threadID-1] = false;
    lock.unlock();
    thread_cv[threadID-1].notify_one();
}

/*
Getters & Setters
*/
template<class Type_Key, class Type_Ts>
inline size_t SWmeta<Type_Key,Type_Ts>::get_meta_size()
{
    return m_numSeg;
}

template<class Type_Key, class Type_Ts>
inline size_t SWmeta<Type_Key,Type_Ts>::get_no_seg()
{
    return m_numSegExist;
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::get_split_error()
{
    return splitError;
}

template<class Type_Key, class Type_Ts>
inline  void SWmeta<Type_Key,Type_Ts>::set_split_error(int error)
{
    splitError = error;
}

template<class Type_Key, class Type_Ts>
void SWmeta<Type_Key,Type_Ts>::print()
{
    printf("start key: %u, slope: %g \n", m_startKey, m_slope);
    if (bitmap_exists(0))
    {
        printf("%u(1) \n", m_keys[0]);
    }
    else
    {
        printf("%u(0) \n", m_keys[0]);
    }

    for (int i = 1; i < m_numSeg; i++)
    {
        cout << ",";
        if (bitmap_exists(i))
        {
            printf("%u(1) \n", m_keys[i]);
        }
        else
        {
            printf("%u(0) \n", m_keys[i]);
        }
    }
    printf("\n");
}

template<class Type_Key, class Type_Ts>
void  SWmeta<Type_Key,Type_Ts>::print_all()
{
    for (int i = 0; i < m_numSeg; i++)
    {
        if (bitmap_exists(i))
        {
            printf("***************************** \n");
            printf("%u(1) \n", m_keys[i]);
            m_ptr[i]->print();
            printf("***************************** \n");
        }
        else
        {
            printf("***************************** \n");
            printf("%u(0) \n", m_keys[i]);
            printf("***************************** \n");
        }
    }
    printf("\n");
}

template <class Type_Key, class Type_Ts>
inline uint64_t SWmeta<Type_Key,Type_Ts>::get_total_size_in_bytes()
{
    uint64_t leafSize = 0;
    for (int i = 0; i < m_numSeg; i++)
    {
        if (bitmap_exists(i))
        {
            leafSize += m_ptr[i]->get_total_size_in_bytes();
        }
    }

    return sizeof(int)*5 + sizeof(double) + sizeof(Type_Key) + sizeof(vector<uint64_t>)*2 + sizeof(uint64_t)*(m_bitmap.size()*2) +
    sizeof(vector<Type_Key>) + sizeof(Type_Key) * m_numSeg + sizeof(vector<SWseg<Type_Key,Type_Ts>*>) + sizeof(SWseg<Type_Key,Type_Ts>*) * m_numSeg + leafSize;
}

template <class Type_Key, class Type_Ts>
inline uint64_t SWmeta<Type_Key,Type_Ts>::get_no_keys(Type_Ts Timestamp)
{
    Type_Ts expiryTime =  ((double)Timestamp - TIME_WINDOW < numeric_limits<Type_Ts>::min()) ? numeric_limits<Type_Ts>::min(): Timestamp - TIME_WINDOW;

    uint64_t cnt = 0;
    for (int i = 0; i < m_numSeg; i++)
    {
        if (bitmap_exists(i))
        {
            cnt += m_ptr[i]->get_no_keys(expiryTime);
        }
    }
    
    return cnt;
}

/*
Bitmap Operations
*/
template<class Type_Key, class Type_Ts>
inline bool SWmeta<Type_Key,Type_Ts>::bitmap_exists(int index) const
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);
    return static_cast<bool>(m_bitmap[bitmapPos] & (1ULL << bitPos));
}

template<class Type_Key, class Type_Ts>
inline bool SWmeta<Type_Key,Type_Ts>::bitmap_exists(vector<uint64_t> & bitmap, int index) const
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);
    return static_cast<bool>(bitmap[bitmapPos] & (1ULL << bitPos));
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_set_bit(int index)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    m_bitmap[bitmapPos] |= (1ULL << bitPos); 
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_set_bit(vector<uint64_t> & bitmap, int index)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    bitmap[bitmapPos] |= (1ULL << bitPos); 
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_erase_bit(int index)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);
    m_bitmap[bitmapPos] &= ~(1ULL << bitPos);
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_erase_bit(vector<uint64_t> & bitmap, int index)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);
    bitmap[bitmapPos] &= ~(1ULL << bitPos);
}


template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::bitmap_closest_left_nongap(int index, int leftBoundary)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    int boundaryBitmapPos = leftBoundary >> 6;

    uint64_t currentBitmap = m_bitmap[bitmapPos];
    currentBitmap &= ((1ULL << (bitPos)) - 1); //Erase all bits after the index (set them to 0)

    while(currentBitmap == 0) 
    //If current set of bitmap is empty (find previous bitmap that is not empty)
    {
        --bitmapPos;
        if (bitmapPos < boundaryBitmapPos )
        {
            return -1; //Out of bounds
        }
        currentBitmap = m_bitmap[bitmapPos];
    }

    // __builtin_clzll(currentBitmap) //Non x86instric.h equivalent for gcc.
    int return_index = (bitmapPos << 6) + (63-static_cast<int>(_lzcnt_u64(currentBitmap)));
    return (return_index < leftBoundary) ? -1 : return_index;
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::bitmap_closest_right_nongap(int index, int rightBoundary)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    int boundaryBitmapPos = rightBoundary >> 6;

    uint64_t currentBitmap = m_bitmap[bitmapPos];
    currentBitmap &= ~((1ULL << (bitPos)) - 1); //Erase all bits before the index (set them to 0)

    while(currentBitmap == 0) 
    //If current set of bitmap is empty (find next bitmap that is not empty)
    {
        ++bitmapPos;
        if (bitmapPos > boundaryBitmapPos)
        {
            return -1;
        }
        currentBitmap = m_bitmap[bitmapPos];
    }

    int return_index = bit_get_index(bitmapPos,bit_extract_rightmost_bit(currentBitmap));
    return (return_index > rightBoundary) ? -1 : return_index;
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::bitmap_closest_gap(int index)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    //If bitmap block is full (check other blocks)
    if (m_bitmap[bitmapPos] == numeric_limits<uint64_t>::max() || //block full
       (bitmapPos == m_bitmap.size()-1 && _mm_popcnt_u64(m_bitmap[bitmapPos]) == (m_numSeg-((m_bitmap.size()-1)<<6))) //last block full
       )
    {
        int maxLeftBlockDist = bitmapPos;
        int maxRightBlockDist =  ((m_numSeg-1) >> 6) - bitmapPos;

        if (m_bitmap.size() == 1)
        {
            return static_cast<int>(_tzcnt_u64(~m_bitmap[bitmapPos]));
        }

        if (m_numSeg == m_numSegExist)
        {
            return m_numSegExist;
        }

        int blockDist = 1;
        int maxBothBlockDist = min<int>(maxLeftBlockDist,maxRightBlockDist);
        
        while (blockDist <= maxBothBlockDist)
        {
            uint64_t rightBitmap = m_bitmap[bitmapPos + blockDist];
            uint64_t leftBitmap = m_bitmap[bitmapPos - blockDist];

            //Both blocks have gaps (find closest)
            if (leftBitmap != numeric_limits<uint64_t>::max() && rightBitmap != numeric_limits<uint64_t>::max())
            {
                int rightGap = ((bitmapPos + blockDist) << 6) + static_cast<int>(_tzcnt_u64(~rightBitmap));
                int leftGap = ((bitmapPos - blockDist + 1) << 6) - static_cast<int>(_lzcnt_u64(~leftBitmap)) - 1;

                if (rightGap - index < index - leftGap)
                {
                    return rightGap; //Can also be last position (need to check outside to append)
                }
                else
                {
                    return leftGap;
                }
            }
            //Gap in right block only (Also need to check leftblock-1 if bitPos < 32)
            else if (rightBitmap != numeric_limits<uint64_t>::max())
            {
                int rightGap = ((bitmapPos + blockDist) << 6) + static_cast<int>(_tzcnt_u64(~rightBitmap));

                if (bitPos < 32 && bitmapPos - blockDist > 0 &&
                    m_bitmap[bitmapPos - blockDist - 1] != numeric_limits<uint64_t>::max())
                {
                    int leftGap = ((bitmapPos - blockDist) << 6) - static_cast<int>(_lzcnt_u64(~m_bitmap[bitmapPos - blockDist - 1])) - 1;

                    if (rightGap - index < index - leftGap)
                    {
                        return rightGap; //Can also be last position (need to check outside to append)
                    }
                    else
                    {
                        return leftGap;
                    }
                }
                else
                {
                    return rightGap;
                }
            }
            //Gap in left block only (Also need to check rightblock+1 if bitPos > 32)
            else if (leftBitmap != numeric_limits<uint64_t>::max())
            {
                int leftGap = ((bitmapPos - blockDist + 1) << 6) - static_cast<int>(_lzcnt_u64(~leftBitmap)) - 1;

                if (bitPos > 32 && bitmapPos + blockDist + 1 < m_bitmap.size() && 
                    m_bitmap[bitmapPos + blockDist + 1] != numeric_limits<uint64_t>::max())
                {
                    int rightGap = ((bitmapPos + blockDist+1) << 6) + static_cast<int>(_tzcnt_u64(~m_bitmap[bitmapPos + blockDist + 1]));

                    if (rightGap - index < index - leftGap)
                    {
                        return rightGap; //Can also be last position (need to check outside to append)
                    }
                    else
                    {
                        return leftGap;
                    } 
                }
                else
                {
                    return leftGap;
                }
            }
            blockDist++;
        }

        if (maxLeftBlockDist > maxRightBlockDist)
        {
            for (int i = bitmapPos - blockDist; i >= 0; i--)
            {
                if (m_bitmap[i] != numeric_limits<uint64_t>::max())
                {
                    return ((i + 1) << 6) - static_cast<int>(_lzcnt_u64(~m_bitmap[i])) - 1;
                }
            }
        }
        else
        {
            int rightBound = ((m_numSeg-1) >> 6);
            for (int i = bitmapPos + blockDist; i <= rightBound; i++)
            {
                if (m_bitmap[i] != numeric_limits<uint64_t>::max())
                {
                    return (i << 6) + static_cast<int>(_tzcnt_u64(~m_bitmap[i]));
                }
            }
        }
        return -1; //Something went wrong (No Gap)

    }
    //There is a gap (somewhere) in current block
    else
    {
        uint64_t currentBitmap = m_bitmap[bitmapPos];

        int minRightGapDist = 64; //Find right closest gap
        uint64_t rightGaps = ~(currentBitmap | ((1ULL << (bitPos)) - 1));
        if (rightGaps)
        {
            minRightGapDist = static_cast<int>(_tzcnt_u64(rightGaps)) - bitPos;
        }
        else if (bitmapPos + 1 != m_bitmap.size())
        {
            minRightGapDist = static_cast<int>(_tzcnt_u64(~m_bitmap[bitmapPos+1])) + 64 - bitPos;
        }

        int minLeftGapDist = 64; //Find left closest gap
        uint64_t leftGaps = (~currentBitmap) & ((1ULL << bitPos) - 1);
        if (leftGaps)
        {
            minLeftGapDist = bitPos - (63 - static_cast<int>(_lzcnt_u64(leftGaps)));
        }
        else if (bitmapPos)
        {
            minLeftGapDist = bitPos + static_cast<int>(_lzcnt_u64(~m_bitmap[bitmapPos-1])) + 1;
        }

        if (minRightGapDist < minLeftGapDist)
        {
            return index + minRightGapDist;  //Can also be last position (need to check outside to append)
        }
        else
        {
            return index - minLeftGapDist;
        }
    }
}

template<class Type_Key, class Type_Ts>
inline int SWmeta<Type_Key,Type_Ts>::bitmap_closest_gap(int index, int leftBoundary, int rightBoundary)
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    int leftIndex;
    uint64_t currentBitmap = m_bitmap[bitmapPos];
    currentBitmap &= ((1ULL << (bitPos)) - 1);
    currentBitmap ^= ((1ULL << (bitPos)) - 1);

    int leftBoundaryBitmapPos = leftBoundary >> 6;
    while(currentBitmap == 0) 
    {
        --bitmapPos;
        if (bitmapPos < leftBoundaryBitmapPos )
        {
            leftIndex = -1; //Out of bounds
            break;
        }
        currentBitmap = m_bitmap[bitmapPos] ^ numeric_limits<uint64_t>::max();
    }
    leftIndex = (bitmapPos << 6) + (63-static_cast<int>(_lzcnt_u64(currentBitmap)));

    if (leftIndex < leftBoundary)
    {
        leftIndex = -1;
    }

    int rightIndex;
    currentBitmap = m_bitmap[bitmapPos];
    currentBitmap &= ~((1ULL << (bitPos)) - 1);
    currentBitmap ^= ~((1ULL << (bitPos)) - 1);

    int rightBoundaryBitmapPos = rightBoundary >> 6;
    while(currentBitmap == 0) 
    {
        bitmapPos++;
        if (bitmapPos > rightBoundaryBitmapPos)
        {
            rightIndex = -1; //Out of bounds
            break;
        }
        currentBitmap = m_bitmap[bitmapPos] ^ numeric_limits<uint64_t>::max();
    }

    rightIndex = bit_get_index(bitmapPos,bit_extract_rightmost_bit(currentBitmap));

    if (rightIndex > rightBoundary)
    {
        rightIndex = -1;
    }

    //Both out of bounds
    if (leftIndex == -1 && rightIndex == -1)
    {
        return -1;
    }

    //One out of bounds
    if (leftIndex == -1 || rightIndex == -1)
    {
        return (leftIndex == -1) ? rightIndex : leftIndex;
    }
    
    //Both in bounds
    if (index - leftIndex < rightIndex - index)
    {
        return leftIndex;
    }
    else
    {
        return rightIndex;
    }
}

template<class Type_Key, class Type_Ts>
inline pair<int,int> SWmeta<Type_Key,Type_Ts>::bitmap_retrain_range(int index)
//Returns [leftExistIndex,rightExistIndex]. 
//If leftExistIndex == rightExistIndex: retrain alone
//Else: retrain with neighbours 
{
    int bitmapPos = index >> 6;
    int bitPos = index - (bitmapPos << 6);

    int tempBitmapPos = bitmapPos;
    uint64_t tempBitmap = (m_retrainBitmap[bitmapPos] & m_bitmap[bitmapPos]);

    int rightIndex = static_cast<int>(_tzcnt_u64(tempBitmap));
    int leftIndex = 63 - static_cast<int>(_lzcnt_u64(tempBitmap));
    
    tempBitmap = (m_retrainBitmap[bitmapPos] ^ m_bitmap[bitmapPos]);
    tempBitmap &= ~((1ULL << (bitPos)) - 1);

    leftIndex = min(leftIndex,static_cast<int>(_tzcnt_u64(tempBitmap))-1);
    if (!static_cast<bool>(m_bitmap[bitmapPos] & (1ULL << leftIndex)))
    {
        uint64_t tempBitmap2 = (m_retrainBitmap[bitmapPos] & m_bitmap[bitmapPos]);
        tempBitmap2 &= ((1ULL << (leftIndex+1)) - 1);
        leftIndex = min(leftIndex, 63 - static_cast<int>(_lzcnt_u64(tempBitmap2)));
    }
    leftIndex += (bitmapPos << 6);

    if (tempBitmap == 0 && bitmapPos != m_bitmap.size()-1)
    {
        tempBitmapPos++;
        tempBitmap = (m_retrainBitmap[tempBitmapPos] ^ m_bitmap[tempBitmapPos]);

        int leftIndexTemp;
        while (tempBitmap == 0 && tempBitmapPos != m_bitmap.size()-1)
        {
            leftIndexTemp = 63 - static_cast<int>(_lzcnt_u64((m_retrainBitmap[tempBitmapPos] & m_bitmap[tempBitmapPos])));

            if (leftIndexTemp != -1)
            {
                leftIndex = (tempBitmapPos << 6) + leftIndexTemp;
            }
            
            tempBitmapPos++;
            tempBitmap = (m_retrainBitmap[tempBitmapPos] ^ m_bitmap[tempBitmapPos]);
        }

        leftIndexTemp = 63 - static_cast<int>(_lzcnt_u64((m_retrainBitmap[tempBitmapPos] & m_bitmap[tempBitmapPos])));

        if (leftIndexTemp != -1 && leftIndexTemp < static_cast<int>(_tzcnt_u64(tempBitmap)))
        {
            leftIndex = (tempBitmapPos << 6) + leftIndexTemp;
        }
    }

    tempBitmap = (m_retrainBitmap[bitmapPos] ^ m_bitmap[bitmapPos]);
    tempBitmap &= ((1ULL << (bitPos)) - 1);
    
    rightIndex = max(rightIndex,64 - static_cast<int>(_lzcnt_u64(tempBitmap)));
    if (!static_cast<bool>(m_bitmap[bitmapPos] & (1ULL << rightIndex)))
    {
        uint64_t tempBitmap2 = (m_retrainBitmap[bitmapPos] & m_bitmap[bitmapPos]);
        tempBitmap2 &= ~((1ULL << (rightIndex)) - 1);
        rightIndex = max(rightIndex,static_cast<int>(_tzcnt_u64(tempBitmap2)));
    }
    rightIndex += (bitmapPos << 6);

    if (tempBitmap == 0 && bitmapPos != 0)
    {
        tempBitmapPos = bitmapPos-1;
        tempBitmap = (m_retrainBitmap[tempBitmapPos] ^ m_bitmap[tempBitmapPos]);

        int rightIndexTemp;
        while (tempBitmap == 0 && tempBitmapPos != 0)
        {
            rightIndexTemp = static_cast<int>(_tzcnt_u64((m_retrainBitmap[tempBitmapPos] & m_bitmap[tempBitmapPos])));

            if (rightIndexTemp != 64)
            {
                rightIndex = (tempBitmapPos << 6) + rightIndexTemp;
            }

            tempBitmapPos--;
            tempBitmap = (m_retrainBitmap[tempBitmapPos] ^ m_bitmap[tempBitmapPos]);
        }

        rightIndexTemp = static_cast<int>(_tzcnt_u64((m_retrainBitmap[tempBitmapPos] & m_bitmap[tempBitmapPos])));

        if (rightIndexTemp != 64 && rightIndexTemp > 63 - static_cast<int>(_lzcnt_u64(tempBitmap)))
        {
            rightIndex = (tempBitmapPos << 6) + rightIndexTemp;
        }
    }

    return make_pair(rightIndex,leftIndex);
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_move_bit_back(int startingIndex, int endingIndex) 
//Inclusive: [startingIndex,endingIndex] and startingIndex < endingIndex
{
    if (static_cast<int>(endingIndex >> 6) == m_bitmap.size())
    {
        m_bitmap.push_back(0); //When end exceeds last pos (end == m_numSeg)
    }

    int bitmapPos = startingIndex >> 6;
    int bitPos = startingIndex - (bitmapPos << 6);

    int bitmapPosEnd = endingIndex >> 6;
    int bitPosEnd = endingIndex - (bitmapPosEnd << 6);

    bool previousBitValue = static_cast<bool>(m_bitmap[bitmapPos] & (1ULL << bitPos));

    while (!(bitmapPos == bitmapPosEnd && bitPos == bitPosEnd))
    {
        if (bitPos == 63)
        {
            bitPos = 0;
            bitmapPos++;
        }
        else
        {
            bitPos++;
        }

        bool temp = static_cast<bool>(m_bitmap[bitmapPos] & (1ULL << bitPos));
        if (previousBitValue)
        {
            m_bitmap[bitmapPos] |= (1ULL << bitPos);
        }
        else
        {
            m_bitmap[bitmapPos] &= ~(1ULL << bitPos);
        }
        previousBitValue = temp;
    }
    return;
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_move_bit_back(vector<uint64_t> & bitmap, int startingIndex, int endingIndex) 
//Inclusive: [startingIndex,endingIndex] and startingIndex < endingIndex
{
    if (static_cast<int>(endingIndex >> 6) == bitmap.size())
    {
        bitmap.push_back(0); //When end exceeds last pos (end == m_numSeg)
    }

    int bitmapPos = startingIndex >> 6;
    int bitPos = startingIndex - (bitmapPos << 6);

    int bitmapPosEnd = endingIndex >> 6;
    int bitPosEnd = endingIndex - (bitmapPosEnd << 6);

    bool previousBitValue = static_cast<bool>(bitmap[bitmapPos] & (1ULL << bitPos));

    while (!(bitmapPos == bitmapPosEnd && bitPos == bitPosEnd))
    {
        if (bitPos == 63)
        {
            bitPos = 0;
            bitmapPos++;
        }
        else
        {
            bitPos++;
        }

        bool temp = static_cast<bool>(bitmap[bitmapPos] & (1ULL << bitPos));
        if (previousBitValue)
        {
            bitmap[bitmapPos] |= (1ULL << bitPos);
        }
        else
        {
            bitmap[bitmapPos] &= ~(1ULL << bitPos);
        }
        previousBitValue = temp;
    }
    return;
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_move_bit_front(int startingIndex, int endingIndex) 
//Inclusive: [startingIndex,endingIndex] and startingIndex > endingIndex
{
    int bitmapPos = startingIndex >> 6;
    int bitPos = startingIndex - (bitmapPos << 6);

    int bitmapPosEnd = endingIndex >> 6;
    int bitPosEnd = endingIndex - (bitmapPosEnd << 6);

    bool previousBitValue = static_cast<bool>(m_bitmap[bitmapPos] & (1ULL << bitPos));

    while (!(bitmapPos == bitmapPosEnd && bitPos == bitPosEnd))
    {
        if (bitPos == 0)
        {
            bitPos = 63;
            bitmapPos--;
        }
        else
        {
            bitPos--;
        }

        bool temp = static_cast<bool>(m_bitmap[bitmapPos] & (1ULL << bitPos));
        if (previousBitValue)
        {
            m_bitmap[bitmapPos] |= (1ULL << bitPos);
        }
        else
        {
            m_bitmap[bitmapPos] &= ~(1ULL << bitPos);
        }
        previousBitValue = temp;
    }
    return;
}

template<class Type_Key, class Type_Ts>
inline void SWmeta<Type_Key,Type_Ts>::bitmap_move_bit_front(vector<uint64_t> & bitmap, int startingIndex, int endingIndex) 
//Inclusive: [startingIndex,endingIndex] and startingIndex > endingIndex
{
    int bitmapPos = startingIndex >> 6;
    int bitPos = startingIndex - (bitmapPos << 6);

    int bitmapPosEnd = endingIndex >> 6;
    int bitPosEnd = endingIndex - (bitmapPosEnd << 6);

    bool previousBitValue = static_cast<bool>(bitmap[bitmapPos] & (1ULL << bitPos));

    while (!(bitmapPos == bitmapPosEnd && bitPos == bitPosEnd))
    {
        if (bitPos == 0)
        {
            bitPos = 63;
            bitmapPos--;
        }
        else
        {
            bitPos--;
        }

        bool temp = static_cast<bool>(bitmap[bitmapPos] & (1ULL << bitPos));
        if (previousBitValue)
        {
            bitmap[bitmapPos] |= (1ULL << bitPos);
        }
        else
        {
            bitmap[bitmapPos] &= ~(1ULL << bitPos);
        }
        previousBitValue = temp;
    }
    return;
}

}
#endif