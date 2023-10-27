#ifndef __PSWIX_SEGMENT_HPP__
#define __PSWIX_SEGMENT_HPP__

#pragma once
#include "helper_p.hpp"

using namespace std;

namespace pswix {

template<class Type_Key, class Type_Ts> class SWmeta;

template<class Type_Key, class Type_Ts>
class SWseg
{
// Variables
private:
    int m_parentIndex;
    int m_rightSearchBound;
    int m_leftSearchBound;
    int m_numPair;
    int m_numPairExist;
    int m_numPairBuffer;
    int m_maxSearchError;
    int m_tuneStage;

    Type_Ts m_maxTimeStamp;
    double m_slope;

    Type_Key m_startKey; // first key of DP
    Type_Key m_currentNodeStartKey; // existing first key of seg/node

    vector<pair<Type_Key,Type_Ts>> m_buffer;
    vector<pair<Type_Key,Type_Ts>> m_localData;

    SWseg<Type_Key,Type_Ts> * m_leftSibling = nullptr;
    SWseg<Type_Key,Type_Ts> * m_rightSibling = nullptr;

// Functions
public:
    SWseg();
    SWseg(int startIndex, int endIndex, double slope, const vector<pair<Type_Key,Type_Ts>> & data);
    SWseg(const pair<Type_Key,Type_Ts> & singleData);

    void local_train(int startIndex, int endIndex, double slope, 
                    const vector<pair<Type_Key,Type_Ts>> & data);
    
    void merge_data(Type_Ts expiryTime, vector<pair<Type_Key,Type_Ts>> & mergedData, int threadLeftBoundary, int threadRightBoundary);
    
    int lookup( int threadLeftBoundary, int threadRightBoundary, 
                Type_Key key, Type_Ts expiryTime, tuple<pswix::seg_update_type,int,Type_Key> & updateSeg);

    int range_search(   int threadLeftBoundary, int threadRightBoundary, 
                        Type_Key lowerBound, Type_Ts expiryTime, Type_Key upperBound, 
                        vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg);

    int range_scan( int threadLeftBoundary, int threadRightBoundary, 
                    Type_Key lowerBound, Type_Ts expiryTime, Type_Key upperBound, 
                    vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg,
                    int startPos = 0, int startBufferPos = 0);

    int insert( int threadLeftBoundary, int threadRightBoundary, Type_Key key, Type_Ts timestamp, Type_Ts expiryTime,
                vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg);
private:
    SWseg<Type_Key,Type_Ts>* insert_neighbour( int threadLeftBoundary, int threadRightBoundary, Type_Key key, Type_Ts timestamp, Type_Ts expiryTime,
                                                vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg, 
                                                bool isLeftNeighbour, SWseg<Type_Key,Type_Ts> * otherNeighbour);

    void insert_current(Type_Key key, Type_Ts timestamp, Type_Ts expiryTime,
                        vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg);

    void insert_model(  int insertionPos, bool addErrorFlag,
                        Type_Key key, Type_Ts timestamp,  Type_Ts expiryTime, 
                        vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg);

    void insert_model_end(  int insertionPos, Type_Key key, Type_Ts timestamp, Type_Ts expiryTime, 
                            vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg);

    void insert_model_append(   int insertionPos, Type_Key key, Type_Ts timestamp, Type_Ts expiryTime, 
                                vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg);
    
    void insert_buffer( int insertionPos, Type_Key key, Type_Ts timestamp, Type_Ts expiryTime,
                        vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg);

public:
    friend class SWmeta<Type_Key,Type_Ts>;
    
    void print();
    uint64_t memory_usage();
    uint64_t get_no_keys(Type_Ts expiryTime);
private:
    tuple<int,int,int> find_predict_pos_bound(Type_Key targetKey);
    void exponential_search_dp_right(Type_Key targetKey, int & foundPos, int maxSearchBound);
    void exponential_search_dp_left(Type_Key targetKey, int & foundPos, int maxSearchBound);
    void binary_search_lower_bound_buffer(Type_Key targetKey, int & foundPos);
    bool index_exists_model(int index, Type_Ts expiryTime);
    bool index_exists_buffer(int index, Type_Ts expiryTime);
    void update_maximium_timestamp(Type_Ts timestamp);
    void update_maximium_search_bound();
    void update_neighour_siblings(int threadLeftBoundary, int threadRightBoundary);
};

/*
Constructors
*/
template<class Type_Key, class Type_Ts>
SWseg<Type_Key,Type_Ts>::SWseg()
:m_rightSearchBound(0), m_leftSearchBound(0), m_numPair(0), m_numPairBuffer(0), m_parentIndex(0), m_maxSearchError(0),
 m_slope(-1), m_maxTimeStamp(numeric_limits<Type_Ts>::min()), 
 m_startKey(numeric_limits<Type_Key>::min()), m_currentNodeStartKey(numeric_limits<Type_Key>::min()) {}

template<class Type_Key, class Type_Ts>
SWseg<Type_Key,Type_Ts>::SWseg(int startIndex, int endIndex, double slope, const vector<pair<Type_Key,Type_Ts>> & data)
:m_rightSearchBound(0), m_leftSearchBound(0), m_numPair(0), m_numPairBuffer(0), m_maxTimeStamp(0), m_parentIndex(0), m_maxSearchError(0)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWseg","Constructor(startIndex, endIndex, slope, data)");
    #endif

    m_buffer.reserve(MAX_BUFFER_SIZE);
    local_train(startIndex, endIndex, slope, data);

    #ifdef TUNE
    m_tuneStage = tuneStage;
    #endif

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWseg","Constructor(startIndex, endIndex, slope, data)");
    #endif
}

template<class Type_Key, class Type_Ts>
SWseg<Type_Key,Type_Ts>::SWseg(const pair<Type_Key,Type_Ts> & singleData)
:m_rightSearchBound(0), m_leftSearchBound(0), m_numPair(0), m_numPairExist(0), m_slope(0), m_parentIndex(0), m_maxSearchError(0)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWseg","Constructor(singleData)");
    #endif

    m_buffer.reserve(MAX_BUFFER_SIZE);

    m_buffer.push_back(singleData);
    m_numPairBuffer = 1;

    m_startKey = singleData.first;
    m_currentNodeStartKey = m_startKey;
    m_maxTimeStamp = singleData.second;

    #ifdef TUNE
    m_tuneStage = tuneStage;
    #endif

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWseg","Constructor(singleData)");
    #endif
}

/*
Local Train Segment with gaps
*/
template<class Type_Key, class Type_Ts>
void SWseg<Type_Key,Type_Ts>::local_train(int startIndex, int endIndex, double slope, const vector<pair<Type_Key,Type_Ts>> & data)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWseg","local_train");
    #endif

    m_startKey = data[startIndex].first;
    m_currentNodeStartKey = m_startKey;
    
    m_slope = slope * 1.05;

    m_localData.push_back(data[startIndex]);
    
    int currentSplitIndex = startIndex + 1;
    int currentInsertionPos = 1;
    int predictedPos;

    m_maxTimeStamp = data[startIndex].second;
    
    while(currentSplitIndex <= endIndex)
    {
        predictedPos = static_cast<int>(floor(m_slope * ((double)data[currentSplitIndex].first - (double)m_startKey)));

        if(predictedPos == currentInsertionPos)
        {

            m_localData.push_back(data[currentSplitIndex]);

            m_maxTimeStamp = (data[currentSplitIndex].second > m_maxTimeStamp) 
                        ? data[currentSplitIndex].second 
                        : m_maxTimeStamp;

            ++currentSplitIndex;
        }
        else if(predictedPos < currentInsertionPos)
        {
            m_localData.push_back(data[currentSplitIndex]);

            m_rightSearchBound = (currentInsertionPos - predictedPos) > m_rightSearchBound ? (currentInsertionPos - predictedPos) : m_rightSearchBound ;

            m_maxTimeStamp = (data[currentSplitIndex].second > m_maxTimeStamp) 
                        ? data[currentSplitIndex].second 
                        : m_maxTimeStamp;
            
            ++currentSplitIndex;
        }
        else
        {
            m_localData.push_back( make_pair(m_localData.back().first,0));

        }
        ++currentInsertionPos;
    }

    m_rightSearchBound += 1;
    m_numPair = m_localData.size();
    m_numPairExist = (endIndex - startIndex + 1);
    
    update_maximium_search_bound();

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWseg","local_train");
    printf("[Debug Info:] m_rightSearchBound = %i \n", m_rightSearchBound);
    printf("[Debug Info:] m_maxTimeStamp = %i \n", m_maxTimeStamp);
    printf("[Debug Info:] m_slope = %g \n", m_slope);
    printf("[Debug Info:] m_numPair = %i \n", m_numPair);
    printf("[Debug Info:] m_numPairExist = %i \n", m_numPairExist);
    printf("[Debug Info:] m_localData.size() = %i \n", m_localData.size());
    #endif
}

/*
Merging data and buffer
*/
template<class Type_Key, class Type_Ts>
void SWseg<Type_Key,Type_Ts>::merge_data(Type_Ts expiryTime, vector<pair<Type_Key,Type_Ts>> & mergedData, int threadLeftBoundary, int threadRightBoundary)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWseg","merge_data");
    #endif

    expiryTime = (expiryTime == 0)?1 :expiryTime; //For non-bulkload insertion
    mergedData.reserve(mergedData.size() + m_numPairExist + m_numPairBuffer);

    int modelIndex = 0;
    int bufferIndex = 0;
    
    while(modelIndex  < m_numPair && bufferIndex  < m_numPairBuffer)
    {
        if (m_localData[modelIndex].first < m_buffer[bufferIndex].first)
        {
            if (m_localData[modelIndex].second >= expiryTime)
            {
                mergedData.push_back(m_localData[modelIndex]);
            }
            ++modelIndex;
        }
        else if (m_localData[modelIndex].first > m_buffer[bufferIndex].first)
        {
            if (m_buffer[bufferIndex].second >= expiryTime)
            {
                mergedData.push_back(m_buffer[bufferIndex]);
            }
            ++bufferIndex;
        }
        else
        {
            LOG_ERROR("ERROR: buffer and m_localData have same value (%u)", m_localData[modelIndex].first);
        }
    }

    while(modelIndex  < m_numPair)
    {
        if (m_localData[modelIndex].second >= expiryTime)
        {
            mergedData.push_back(m_localData[modelIndex]);
        }
        ++modelIndex;
    }

    while (bufferIndex < m_numPairBuffer)
    {
        if (m_buffer[bufferIndex].second >= expiryTime)
        {
            mergedData.push_back(m_buffer[bufferIndex]);
        }
        ++bufferIndex;
    }

    update_neighour_siblings(threadLeftBoundary, threadRightBoundary);
    m_localData.clear();
    m_buffer.clear();

    #ifdef TUNE
    segLengthMerge += m_numPair + m_numPairBuffer;
    #endif

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWseg","merge_data");
    #endif
}

/*
Point Lookup
*/
template<class Type_Key, class Type_Ts>
int SWseg<Type_Key,Type_Ts>::lookup(   int threadLeftBoundary, int threadRightBoundary, 
                                        Type_Key key, Type_Ts expiryTime, tuple<pswix::seg_update_type,int,Type_Key> & updateSeg)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWseg","lookup");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == key) {DEBUG_ENTER_FUNCTION("SWseg","lookup");}
    #endif

    if (m_maxTimeStamp >= expiryTime) //Segment as a whole did not expire
    {
        if (m_numPair)
        {
            int actualPos, predictPosMin, predictPosMax;
            tie(actualPos, predictPosMin, predictPosMax) = find_predict_pos_bound(key);

            if (predictPosMin < predictPosMax)
            {
                actualPos = actualPos < predictPosMin ? predictPosMin : actualPos;
                actualPos = actualPos > predictPosMax ? predictPosMax : actualPos;

                if (key < m_localData[actualPos].first) // Left of predictedPos
                {  
                    exponential_search_dp_left(key, actualPos, actualPos-predictPosMin);
                }
                else if (key > m_localData[actualPos].first)
                {
                    exponential_search_dp_right(key, actualPos, predictPosMax-actualPos);
                }
            }
            else if (predictPosMin != predictPosMax)
            {
                actualPos = (!predictPosMin)? 0 : m_numPair-1;
            }

            if (m_localData[actualPos].second && m_localData[actualPos].second >= expiryTime)
            {
                return (m_localData[actualPos].first == key);
            }
            else
            {
                if (m_localData[actualPos].second)
                {
                    m_localData[actualPos].second = 0;
                    --m_numPairExist;
                }
            }

            if ((double)m_numPairExist/m_numPair < 0.5)
            {
                updateSeg = make_tuple(pswix::seg_update_type::RETRAIN,m_parentIndex,m_currentNodeStartKey);
            }
        }

        if (m_numPairBuffer)
        {
            auto it = lower_bound(m_buffer.begin(),m_buffer.end(),key,
                    [](const pair<Type_Key,Type_Ts>& data, Type_Key value)
                    {
                        return data.first < value;
                    });
        
            if (it != m_buffer.end())
            {
                if (it->second >= expiryTime)
                {
                    return (it->first == key);
                }
                else
                {
                    m_buffer.erase(it);
                    --m_numPairBuffer;
                }
            }
        }
    }
    else //Entire segment expired
    {        
        update_neighour_siblings(threadLeftBoundary, threadRightBoundary);
        updateSeg = make_tuple(pswix::seg_update_type::DELETE,m_parentIndex,m_currentNodeStartKey);
    }

    return 0;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWseg","lookup");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == key) {DEBUG_EXIT_FUNCTION("SWseg","lookup");}
    #endif
}

/*
Range Query
*/
template<class Type_Key, class Type_Ts>
int SWseg<Type_Key,Type_Ts>::range_search( int threadLeftBoundary, int threadRightBoundary, 
                                            Type_Key lowerBound, Type_Ts expiryTime, Type_Key upperBound, 
                                            vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg)
{    
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWseg","range_search");
    printf("[Debug Info:] lowerBound = %u, expiryTime = %u \n", lowerBound, expiryTime);
    printf("[Debug Info:] upperBound = %u \n", upperBound);
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == lowerBound) 
    {
        DEBUG_ENTER_FUNCTION("SWseg","range_search");
        printf("[Debug Info:] lowerBound = %u, expiryTime = %u \n", lowerBound, expiryTime);
        printf("[Debug Info:] upperBound = %i \n", upperBound);
    }
    #endif

    if (m_maxTimeStamp >= expiryTime) //Segment as a whole did not expire
    {
        int actualPos = 0, bufferPos = 0;
        if (m_numPair) //Search Data
        {
            int predictPosMin, predictPosMax;
            tie(actualPos, predictPosMin, predictPosMax) = find_predict_pos_bound(lowerBound);

            #if defined DEBUG 
            printf("[Debug Info:] predictPosMin = %i \n", predictPosMin);
            printf("[Debug Info:] predictPosMax = %i \n", predictPosMax);
            #endif 

            #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
            if(DEBUG_KEY == lowerBound)
            {
                printf("[Debug Info:] predictPosMin = %i \n", predictPosMin);
                printf("[Debug Info:] predictPosMax = %i \n", predictPosMax);
            }
            #endif

            if (predictPosMin < predictPosMax)
            {
                actualPos = actualPos < predictPosMin ? predictPosMin : actualPos;
                actualPos = actualPos > predictPosMax ? predictPosMax : actualPos;

                if (lowerBound < m_localData[actualPos].first) // Left of predictedPos
                {  
                    exponential_search_dp_left(lowerBound, actualPos, actualPos-predictPosMin);
                }
                else if (lowerBound > m_localData[actualPos].first)
                {
                    exponential_search_dp_right(lowerBound, actualPos, predictPosMax-actualPos);
                }
            }
            else if (predictPosMin != predictPosMax)
            {
                actualPos = (!predictPosMin)? 0 : m_numPair-1;
            }
        }

        if (m_numPairBuffer)
        {
            binary_search_lower_bound_buffer(lowerBound,bufferPos);

            #if defined DEBUG 
            printf("[Debug Info:] bufferPos = %i \n", bufferPos);
            #endif 

            #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
            if(DEBUG_KEY == lowerBound)
            {
                printf("[Debug Info:] bufferPos = %i \n", bufferPos);
            }
            #endif
        }
        return range_scan(threadLeftBoundary, threadRightBoundary, lowerBound, expiryTime, upperBound, updateSeg, actualPos, bufferPos);
    }

    return range_scan(threadLeftBoundary, threadRightBoundary, lowerBound, expiryTime, upperBound, updateSeg); //Entire segment expired
    
    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWseg","range_search");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == lowerBound) {DEBUG_EXIT_FUNCTION("SWseg","range_search");}
    #endif
}

template<class Type_Key, class Type_Ts>
int SWseg<Type_Key,Type_Ts>::range_scan(   int threadLeftBoundary, int threadRightBoundary, 
                                            Type_Key lowerBound, Type_Ts expiryTime, Type_Key upperBound, 
                                            vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg, 
                                            int startPos, int startBufferPos)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWseg","range_scan");
    printf("[Debug Info:] startSearchPos = %i \n", startPos);
    printf("[Debug Info:] startSearchBufferPos = %i \n", startBufferPos);
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == lowerBound) 
    {
        DEBUG_ENTER_FUNCTION("SWseg","range_scan");
        printf("[Debug Info:] m_currentNodeStartKey = %u \n", m_currentNodeStartKey);
        printf("[Debug Info:] m_startKey = %u \n", m_startKey);
        printf("[Debug Info:] m_maxTimeStamp = %u \n", m_maxTimeStamp);
        printf("[Debug Info:] startSearchPos = %i \n", startPos);
        printf("[Debug Info:] startSearchBufferPos = %i \n", startBufferPos);
        printf("[Debug Info:] m_numPair = %i, %i \n", m_numPair , m_localData.size());
        printf("[Debug Info:] m_numPairExist = %i \n", m_numPairExist);
        printf("[Debug Info:] m_numPairBuffer = %i, %i \n", m_numPairBuffer, m_buffer.size());
    }
    #endif

    #ifdef TUNE
    segScanNoSeg++;
    #endif

    int count = 0;
    if (m_maxTimeStamp >= expiryTime) 
    {
        
        //Search Data
        while (startPos < m_numPair && m_localData[startPos].first <= upperBound)
        {
            if (m_localData[startPos].second && m_localData[startPos].second >= expiryTime)
            {
                ++count;
            }
            else
            {
                if (m_localData[startPos].second)
                {
                    m_localData[startPos].second = 0;
                    --m_numPairExist;
                }
            }
            ++startPos;
        }
        
        //Search Buffer
        bool deleteFromBuffer = false;
        while(startBufferPos < m_numPairBuffer && m_buffer[startBufferPos].first <= upperBound)
        {
            if (m_buffer[startBufferPos].second && m_buffer[startBufferPos].second >= expiryTime)
            {
                ++count;
            }
            else
            {
                deleteFromBuffer = true;
                m_buffer[startBufferPos].first = 0;
                m_buffer[startBufferPos].second = 0;
            }
            ++startBufferPos;
        }
        
        if (deleteFromBuffer)
        {
           pair<Type_Key,Type_Ts> deletePair = make_pair(0,0);
           m_buffer.erase(remove(m_buffer.begin(), m_buffer.end(), deletePair), m_buffer.end());
           m_numPairBuffer = m_buffer.size();
        }
        
        if ((double)m_numPairExist/m_numPair < 0.5)
        {
            updateSeg.push_back(make_tuple(pswix::seg_update_type::RETRAIN,m_parentIndex,m_currentNodeStartKey));
        }
    }
    else //Entire segment expired
    {
        update_neighour_siblings(threadLeftBoundary, threadRightBoundary);
        updateSeg.push_back(make_tuple(pswix::seg_update_type::DELETE,m_parentIndex,m_currentNodeStartKey));
    }

    #ifdef DEBUG 
    printf("[Debug Info:] Range Scan: Go to Sibling \n");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == lowerBound){printf("[Debug Info:] Range Scan: Go to Sibling \n");}
    #endif
    
    if (m_parentIndex < threadRightBoundary)
    {
        if(m_rightSibling && m_rightSibling->m_currentNodeStartKey <= upperBound)
        {
            count += m_rightSibling->range_scan(threadLeftBoundary, threadRightBoundary, lowerBound, expiryTime, upperBound, updateSeg);
        }
    }
    
    #ifdef DEBUG 
    printf("[Debug Info:] Range Scan: Back from Sibling \n");
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == lowerBound){printf("[Debug Info:] Range Scan: Back from Sibling \n");}
    #endif

    return count;

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWseg","range_scan");
    printf("[Debug Info:] stop DP pos = %i \n", startPos);
    printf("[Debug Info:] stop buffer pos = %i \n", startBufferPos);
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == lowerBound) 
    {
        DEBUG_EXIT_FUNCTION("SWseg","range_scan");
        printf("[Debug Info:] stop DP pos = %i \n", startPos);
        printf("[Debug Info:] stop buffer pos = %i \n", startBufferPos);
    }
    #endif
}

/*
Insertions
*/
template <class Type_Key, class Type_Ts>
int SWseg<Type_Key,Type_Ts>::insert(   int threadLeftBoundary, int threadRightBoundary, 
                                        Type_Key key, Type_Ts timestamp, Type_Ts expiryTime,
                                        vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("SWseg","insert");
    printf("[Debug Info:] key = %lu, timestamp = %lu \n" , key, timestamp);
    printf("[Debug Info:] m_startKey = %lu \n ", m_startKey);
    printf("[Debug Info:] m_currentNodeStartKey = %lu \n ", m_currentNodeStartKey);
    printf("[Debug Info:] m_numPair = %i \n ", m_numPair);
    printf("[Debug Info:] m_numPairExist = %i \n ", m_numPairExist);
    printf("[Debug Info:] m_numPairBuffer =%i \n ", m_numPairBuffer); 
    printf("[Debug Info:] m_maxTimeStamp = %lu \n ", m_maxTimeStamp);
    printf("[Debug Info:] m_slope = %g \n ", m_slope);
    printf("[Debug Info:] m_rightSearchBound = %i \n ", m_rightSearchBound);
    printf("[Debug Info:] m_leftSearchBound = %i \n ", m_leftSearchBound);
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
    if(DEBUG_KEY == key)
    {
        DEBUG_ENTER_FUNCTION("SWseg","insert");
        printf("[Debug Info:] key = %lu, timestamp = %lu \n" , key, timestamp);
        printf("[Debug Info:] m_startKey = %lu \n ", m_startKey);
        printf("[Debug Info:] m_currentNodeStartKey = %lu \n ", m_currentNodeStartKey);
        printf("[Debug Info:] m_numPair = %i \n ", m_numPair);
        printf("[Debug Info:] m_numPairExist = %i \n ", m_numPairExist);
        printf("[Debug Info:] m_numPairBuffer =%i \n ", m_numPairBuffer); 
        printf("[Debug Info:] m_maxTimeStamp = %lu \n ", m_maxTimeStamp);
        printf("[Debug Info:] m_slope = %g \n ", m_slope);
        printf("[Debug Info:] m_rightSearchBound = %i \n ", m_rightSearchBound);
        printf("[Debug Info:] m_leftSearchBound = %i \n ", m_leftSearchBound);
    }
    #endif

    if (m_maxTimeStamp >= expiryTime) //Segment still valid
    {
        updateSeg.reserve(1);
        insert_current(key, timestamp, expiryTime, updateSeg);
        update_maximium_timestamp(timestamp);

        if (updateSeg.size() == 0 && (double)m_numPairExist/m_numPair < 0.5)
        {
            updateSeg.push_back(make_tuple(pswix::seg_update_type::RETRAIN,m_parentIndex,m_currentNodeStartKey));
        }
    }
    else //Segment expired
    {       
        updateSeg.reserve(2);
        bool leftInsertFlag = ((m_parentIndex > threadLeftBoundary) && m_leftSibling); //At left boundary do not insert into left sibling
        bool rightInsertFlag = ((m_parentIndex < threadRightBoundary) && m_rightSibling); //At right boundary do not insert into right sibling

        if (leftInsertFlag) //Left Sibling accessible 
        {
                #ifdef DEBUG
                printf("[Debug Info:] Insert into left neighbour \n");
                #endif

                #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
                if(DEBUG_KEY == key)
                {
                    printf("[Debug Info:] Insert into left neighbour \n");
                }
                #endif

                SWseg<Type_Key,Type_Ts>* rightSibling = (rightInsertFlag && m_rightSibling)? m_rightSibling : nullptr;
                SWseg<Type_Key,Type_Ts>* insertedSegPointer = m_leftSibling->insert_neighbour(threadLeftBoundary,threadRightBoundary,
                                                                key, timestamp, expiryTime, updateSeg,
                                                                true,rightSibling);              

                updateSeg.push_back(make_tuple(pswix::seg_update_type::DELETE,m_parentIndex,m_currentNodeStartKey));
                if (rightInsertFlag)
                {
                    if (insertedSegPointer != NULL)
                    {
                        m_rightSibling->m_leftSibling = insertedSegPointer;
                    }
                    else
                    {
                        insertedSegPointer = rightSibling->insert_neighbour(threadLeftBoundary,threadRightBoundary,
                                                                key, timestamp, expiryTime, updateSeg,
                                                                false, nullptr);
                        if (insertedSegPointer == NULL)
                        {
                            LOG_ERROR("ERROR: All segments in partition is empty, abort insertion \n");
                            abort();
                        }
                    }
                }
        }
        else if (rightInsertFlag) //Right Sibling accessible
        {
                #ifdef DEBUG
                printf("[Debug Info:] Insert into right neighbour \n");
                #endif

                #if defined(DEBUG_KEY) || defined(DEBUG_TS) 
                if(DEBUG_KEY == key)
                {
                    printf("[Debug Info:] Insert into right neighbour \n");
                }
                #endif

                updateSeg.push_back(make_tuple(pswix::seg_update_type::DELETE,m_parentIndex,m_currentNodeStartKey));
                SWseg<Type_Key,Type_Ts>* insertedSegPointer = m_rightSibling->insert_neighbour(threadLeftBoundary,threadRightBoundary,
                                                                key, timestamp, expiryTime, updateSeg,
                                                                false,nullptr);
                if (insertedSegPointer == NULL)
                {
                    LOG_ERROR("ERROR: All segments in partition is empty, abort insertion \n");
                    abort();
                }
        }
        else //Isolated Segment
        {
            LOG_ERROR("ERROR: No left or right sibling to insert into \n");
            abort();
        }
    }

    #ifdef DEBUG
    DEBUG_EXIT_FUNCTION("SWseg","insert");
    printf("[Debug Info:] Printing m_localData after insertion : \n");
    printf("[Debug Info:] m_numPair = %i \n", m_numPair);
    printf("[Debug Info:] m_numPairExist = %i \n", m_numPairExist);
    printf("[Debug Info:] m_numPairBuffer =%i \n", m_numPairBuffer); 
    printf("[Debug Info:] m_maxTimeStamp = %i \n", m_maxTimeStamp);
    printf("[Debug Info:] m_rightSearchBound = %i \n", m_rightSearchBound);
    printf("[Debug Info:] m_leftSearchBound = %i \n", m_leftSearchBound);
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS)
    if(DEBUG_KEY == key)
    {
        DEBUG_EXIT_FUNCTION("SWseg","insert");
        printf("[Debug Info:] Printing m_localData after insertion : \n");
        printf("[Debug Info:] m_numPair = %i \n", m_numPair);
        printf("[Debug Info:] m_numPairExist = %i \n", m_numPairExist);
        printf("[Debug Info:] m_numPairBuffer =%i \n", m_numPairBuffer); 
        printf("[Debug Info:] m_maxTimeStamp = %i \n", m_maxTimeStamp);
        printf("[Debug Info:] m_rightSearchBound = %i \n", m_rightSearchBound);
        printf("[Debug Info:] m_leftSearchBound = %i \n", m_leftSearchBound);
    }
    #endif

    return 1;
}

template <class Type_Key, class Type_Ts>
SWseg<Type_Key,Type_Ts>* SWseg<Type_Key,Type_Ts>::insert_neighbour( int threadLeftBoundary, int threadRightBoundary, 
                                                Type_Key key, Type_Ts timestamp, Type_Ts expiryTime,
                                                vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg,
                                                bool isLeftNeighbour, SWseg<Type_Key,Type_Ts> * otherNeighbour)
{
    if (m_maxTimeStamp >= expiryTime) //Segment still valid
    {
        if (isLeftNeighbour)
        {
            m_buffer.push_back(make_pair(key,timestamp));
            ++m_numPairBuffer;
            update_maximium_timestamp(timestamp);

            m_rightSibling = otherNeighbour;
            if (m_numPairBuffer > MAX_BUFFER_SIZE)
            {
                updateSeg.push_back(make_tuple(pswix::seg_update_type::RETRAIN,m_parentIndex,m_currentNodeStartKey));
            }
            return this;
        }
        else //isRightNeighbor
        {
            int updateSegSize = updateSeg.size();
            insert_buffer(0, key, timestamp, expiryTime, updateSeg);
            m_currentNodeStartKey = key;
            update_maximium_timestamp(timestamp);

            m_leftSibling = otherNeighbour;
            if (updateSegSize == updateSeg.size())
            {
                updateSeg.push_back(make_tuple(pswix::seg_update_type::REPLACE,m_parentIndex,key));
            }
            return this;
        }
    }
    else //Keep passing to neighbours (if within thread boundary)
    {
        SWseg<Type_Key,Type_Ts>* insertedSegPointer = nullptr; 
        if (isLeftNeighbour)
        {
            if (m_leftSibling && m_parentIndex > threadLeftBoundary)
            {
                insertedSegPointer = m_leftSibling->insert_neighbour(threadLeftBoundary, threadRightBoundary, 
                                        key, timestamp, expiryTime, updateSeg, isLeftNeighbour, otherNeighbour);
            }
            updateSeg.push_back(make_tuple(pswix::seg_update_type::DELETE,m_parentIndex,m_currentNodeStartKey));
        }
        else //isRightNeighbor
        {
            if (m_rightSibling && m_parentIndex < threadRightBoundary)
            {
                insertedSegPointer = m_rightSibling->insert_neighbour(threadLeftBoundary, threadRightBoundary, 
                                    key, timestamp, expiryTime, updateSeg, isLeftNeighbour, otherNeighbour);
            }
            updateSeg.push_back(make_tuple(pswix::seg_update_type::DELETE,m_parentIndex,m_currentNodeStartKey));
        }
        return insertedSegPointer;
    }
}

template <class Type_Key, class Type_Ts>
void SWseg<Type_Key,Type_Ts>::insert_current(  Type_Key key, Type_Ts timestamp, Type_Ts expiryTime,
                                                vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg)
{
    if (key < m_startKey || !m_numPair)
    {
        #ifdef DEBUG
        printf("[Debug Info:] key < m_startKey or empty DALarray, insert into buffer \n");
        #endif

        #if defined(DEBUG_KEY) || defined(DEBUG_TS)
        if(DEBUG_KEY == key)
        {
            printf("[Debug Info:] key < m_startKey or empty DALarray, insert into buffer \n");
        }
        #endif
        
        //NOTE: There may be cases where you insert into the first segment before m_startKey, then do we need to update the tree? 
        return insert_buffer(-1, key, timestamp, expiryTime, updateSeg);
    }

    //Insert with model prediction
    int insertionPos, predictPosMin, predictPosMax;
    tie(insertionPos, predictPosMin, predictPosMax) = find_predict_pos_bound(key);

    if (predictPosMin < predictPosMax)
    {
        insertionPos = insertionPos < predictPosMin ? predictPosMin : insertionPos;
        insertionPos = insertionPos > predictPosMax ? predictPosMax : insertionPos;

        if (key < m_localData[insertionPos].first) // Left of predictedPos
        {
            exponential_search_dp_left(key, insertionPos, insertionPos-predictPosMin);
        }
        else if (key > m_localData[insertionPos].first) //Right of predictedPos
        {
            exponential_search_dp_right(key, insertionPos, predictPosMax-insertionPos);
        }

        bool addErrorFlag = (insertionPos > predictPosMax);

        //Insert if insertion pos is less than last index
        if (insertionPos < m_numPair)
        {                
            insert_model(insertionPos, addErrorFlag, key, timestamp, expiryTime, updateSeg);
        }
        //Else append and increment m_rightSearchBound
        else
        {
            #ifdef DEBUG
            printf("[Debug Info:] appending key to end of DALarray \n");
            #endif

            #if defined(DEBUG_KEY) || defined(DEBUG_TS)
            if(DEBUG_KEY == key)
            {
                printf("[Debug Info:] appending key to end of DALarray \n");
            }
            #endif
            
            m_localData.push_back(make_pair(key,timestamp));
            ++m_numPair;
            ++m_numPairExist;
            ++m_rightSearchBound;
        }

    }
    //Check last position to append or insert
    else if (predictPosMin == predictPosMax)
    {   
        insert_model_end(predictPosMin, key, timestamp, expiryTime, updateSeg);

    }
    //Insertion is append type 
    else
    {
        insert_model_append(predictPosMin + m_leftSearchBound, key, timestamp, expiryTime, updateSeg);
    }
    update_maximium_search_bound();

    return;
}


template <class Type_Key, class Type_Ts>
void SWseg<Type_Key,Type_Ts>::insert_model(int insertionPos, bool addErrorFlag, Type_Key key, Type_Ts timestamp, Type_Ts expiryTime,
                                            vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg)
{
    //If insertionPos is not a gap
    if (index_exists_model(insertionPos, expiryTime))
    {
        if (m_leftSearchBound + m_rightSearchBound >= m_maxSearchError)
        {
            return insert_buffer(-1, key, timestamp, expiryTime, updateSeg);
        }

        /*
        Find shifting position
            First find right gap (shiftOrNot = true)
                It might reach the end and find no gap. (That is fine, record the distance to the end) (shiftOrNot = false)
            Then find left gap that is closer to the insertionPos (shiftOrNot = true)

        If no gap within ShiftCost (left and right)
            Insert to Buffer

        If (shiftOrNot = true)
            "Move" keys using the move() function and insert into insertion position
                increment m_rightSearchBound or m_leftSearchBound depending on the location of the gap.
        Else
            "Insert" keys using the v.insert() function (no gaps, and closer to the end of the DALarray)
                increment m_rightSearchBound
        */

        int gapPos = -1;
        bool shiftOrNot = false;
        int rightGapPos, leftGapPos;

        //Find Left Gap
        int minDiffGap = (insertionPos-MAX_BUFFER_SIZE < 0)? 0 : insertionPos-MAX_BUFFER_SIZE;
        for (leftGapPos = insertionPos; leftGapPos >= minDiffGap; --leftGapPos)
        {
            if (!index_exists_model(leftGapPos, expiryTime))
            {
                gapPos = leftGapPos;
                shiftOrNot = true;
                break;
            }
        }

        //Find Right Gap
        minDiffGap = (gapPos == -1) ? insertionPos + MAX_BUFFER_SIZE : insertionPos + (insertionPos - gapPos);
        minDiffGap = (minDiffGap > m_numPair) ? m_numPair : minDiffGap;
        for (rightGapPos = insertionPos; rightGapPos < minDiffGap; ++rightGapPos)
        {
            if (!index_exists_model(rightGapPos, expiryTime))
            {
                gapPos = rightGapPos;
                shiftOrNot = true;
                break;
            }
        }

        //No Gap and reached the end (end is still within MAX_BUFFER_SIZE)
        if (rightGapPos == m_numPair)
        {
            gapPos = m_numPair;
            shiftOrNot = false;
        }
        
        //Insert After finding Gap
        if (gapPos == -1)
        {   
            #ifdef DEBUG 
            printf("[Debug Info:] No gaps within shift cost, insert to buffer \n");
            #endif

            #if defined(DEBUG_KEY) || defined(DEBUG_TS)
            if(DEBUG_KEY == key)
            {
                printf("[Debug Info:] No gaps within shift cost, insert to buffer \n");
            }
            #endif

            return insert_buffer(-1, key, timestamp, expiryTime, updateSeg);
        }

        //Gap exist within shiftcost
        if (shiftOrNot)
        {
            if (insertionPos < gapPos)
            {
                #ifdef DEBUG 
                printf("[Debug Info:] Shift towards a gap right of the insertionPos \n");
                printf("[Debug Info:] InsertionPos = %i \n" , insertionPos);
                printf("[Debug Info:] key at InsertionPos = %u \n" , m_localData[insertionPos].first);
                printf("[Debug Info:] Shift Bound = (%i, %i) \n" , insertionPos, gapPos);
                #endif

                #if defined(DEBUG_KEY) || defined(DEBUG_TS)
                if(DEBUG_KEY == key)
                {
                    printf("[Debug Info:] Shift towards a gap right of the insertionPos \n");
                    printf("[Debug Info:] InsertionPos = %i \n" , insertionPos);
                    printf("[Debug Info:] key at InsertionPos = %u \n" , m_localData[insertionPos].first);
                    printf("[Debug Info:] Shift Bound = (%i, %i) \n" , insertionPos, gapPos);
                }
                #endif

                vector<pair<Type_Key,Type_Ts>> tempMove(m_localData.begin()+insertionPos,m_localData.begin()+gapPos);
                move(tempMove.begin(),tempMove.end(),m_localData.begin()+insertionPos+1);
                
                m_localData[insertionPos] = make_pair(key,timestamp);
                ++m_numPairExist;
                ++m_rightSearchBound;

            }
            //Gap on the left -> move from insertion pos -1 and insert at insertion pos - 1
            else 
            {
                #ifdef DEBUG
                printf("[Debug Info:] Shift towards a gap left of the insertionPos \n");
                printf("[Debug Info:] InsertionPos = %i \n", insertionPos-1 );
                printf("[Debug Info:] key at InsertionPos = %u \n", m_localData[insertionPos-1 ].first);
                printf("[Debug Info:] Shift Bound = (%i, %i) \n", gapPos, insertionPos-1);
                #endif

                #if defined(DEBUG_KEY) || defined(DEBUG_TS)
                if(DEBUG_KEY == key)
                {
                    printf("[Debug Info:] Shift towards a gap left of the insertionPos \n");
                    printf("[Debug Info:] InsertionPos = %i \n", insertionPos-1 );
                    printf("[Debug Info:] key at InsertionPos = %u \n", m_localData[insertionPos-1 ].first);
                    printf("[Debug Info:] Shift Bound = (%i, %i) \n", gapPos, insertionPos-1);
                }
                #endif

                --insertionPos;
                vector<pair<Type_Key,Type_Ts>> tempMove(m_localData.begin()+gapPos+1,m_localData.begin()+insertionPos+1);
                move(tempMove.begin(),tempMove.end(),m_localData.begin()+gapPos);

                m_localData[insertionPos] = make_pair(key,timestamp);
                ++m_numPairExist;
                ++m_leftSearchBound;
            }                            
        }
        else
        {
            #ifdef DEBUG 
            printf("[Debug Info:] Minimium distance is to the end of the DALarray, insert at insertion positon \n");
            printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
            printf("[Debug Info:] key at InsertionPos = %u \n", m_localData[insertionPos].first);
            #endif

            #if defined(DEBUG_KEY) || defined(DEBUG_TS)
            if(DEBUG_KEY == key)
            {
                printf("[Debug Info:] Minimium distance is to the end of the DALarray, insert at insertion positon \n");
                printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
                printf("[Debug Info:] key at InsertionPos = %u \n", m_localData[insertionPos].first);
            }
            #endif
            
            //Insert without gaps
            m_localData.insert(m_localData.begin()+insertionPos,make_pair(key,timestamp));
            ++m_numPair;
            ++m_numPairExist;
            ++m_rightSearchBound;
        }
    }
    //If insertion pos is a gap
    else
    {
        if (addErrorFlag && m_leftSearchBound + m_rightSearchBound >= m_maxSearchError)
        {
            return insert_buffer(-1, key, timestamp, expiryTime, updateSeg);
        }
        
        #ifdef DEBUG 
        printf("[Debug Info:] Insertion Position is a Gap, change all empty keys after insertionPos to have key = %u \n",key);
        printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
        printf("[Debug Info:] key at InsertionPos = %u \n", m_localData[insertionPos].first);
        #endif 

        #if defined(DEBUG_KEY) || defined(DEBUG_TS)
        if(DEBUG_KEY == key)
        {
            printf("[Debug Info:] Insertion Position is a Gap, change all empty keys after insertionPos to have key = %u \n",key);
            printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
            printf("[Debug Info:] key at InsertionPos = %u \n", m_localData[insertionPos].first);
        }
        #endif
        
        m_localData[insertionPos].first = key;
        m_localData[insertionPos].second = timestamp;
        ++m_numPairExist;
        int nextPos = insertionPos + 1;
        while (nextPos < m_numPair && m_localData[nextPos].first < key)
        {
            m_localData[nextPos].first = key;
            ++nextPos;
        }
        if (addErrorFlag) {++m_rightSearchBound;}
    }
}


template <class Type_Key, class Type_Ts>
void SWseg<Type_Key,Type_Ts>::insert_model_end(int insertionPos, Type_Key key, Type_Ts timestamp, Type_Ts expiryTime, 
                                                vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg)
{
    //If last key in not a gap
    if (index_exists_model(insertionPos, expiryTime))
    {
        if (key < m_localData[insertionPos].first)
        {
            if (m_leftSearchBound + m_rightSearchBound >= m_maxSearchError)
            {
                return insert_buffer(-1, key, timestamp, expiryTime, updateSeg);
            }

            #if defined DEBUG 
            printf("[Debug Info:] key is smaller than last key, insert into last key position \n");
            printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
            printf("[Debug Info:] key at InsertionPos = %u \n", m_localData[insertionPos].first);
            #endif 

            #if defined(DEBUG_KEY) || defined(DEBUG_TS)
            if(DEBUG_KEY == key)
            {
                printf("[Debug Info:] key is smaller than last key, insert into last key position \n");
                printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
                printf("[Debug Info:] key at InsertionPos = %u \n", m_localData[insertionPos].first);
            }
            #endif
            
            //Insert and increment m_rightSearchBound
            m_localData.insert(m_localData.end()-1,make_pair(key,timestamp));
            ++m_numPair;
            ++m_numPairExist;
            ++m_rightSearchBound;
        }
        else //key > m_localData[insertionPos].first 
        {
            #ifdef DEBUG 
            printf("[Debug Info:] key (%u) is larger than last key, append after the last position \n", key);
            printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
            printf("[Debug Info:] key at InsertionPos = %u \n", m_localData[insertionPos].first);
            #endif

            #if defined(DEBUG_KEY) || defined(DEBUG_TS)
            if(DEBUG_KEY == key)
            {
                printf("[Debug Info:] key (%u) is larger than last key, append after the last position \n", key);
                printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
                printf("[Debug Info:] key at InsertionPos = %u \n", m_localData[insertionPos].first);
            }
            #endif

            //Append and increment m_rightSearchBound
            m_localData.push_back(make_pair(key,timestamp));
            ++m_numPair;
            ++m_numPairExist;
        }
    }
    //Last key is a gap
    else
    {
        #ifdef DEBUG 
        printf("[Debug Info:] Last key is a gap \n");
        printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
        printf("[Debug Info:] key at InsertionPos = %u \n", m_localData[insertionPos].first);
        #endif

        #if defined(DEBUG_KEY) || defined(DEBUG_TS)
        if(DEBUG_KEY == key)
        {
            printf("[Debug Info:] Last key is a gap \n");
            printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
            printf("[Debug Info:] key at InsertionPos = %u \n", m_localData[insertionPos].first);
        }
        #endif

        m_localData[insertionPos].first = key;
        m_localData[insertionPos].second = timestamp;
        ++m_numPairExist;
    }
}

template <class Type_Key, class Type_Ts>
void SWseg<Type_Key,Type_Ts>::insert_model_append( int insertionPos, Type_Key key, Type_Ts timestamp, Type_Ts expiryTime,
                                                    vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg)
{
    //If gaps makes the segment very empty.
    if (((double)m_numPairExist+1) / (insertionPos+1) < 0.5)
    {
        #if defined DEBUG 
        printf("[Debug Info:] Append type insertion - number of gaps exceed threshold - insert into buffer \n");
        printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
        #endif

        #if defined(DEBUG_KEY) || defined(DEBUG_TS)
        if(DEBUG_KEY == key)
        {
            printf("[Debug Info:] Append type insertion - number of gaps exceed threshold - insert into buffer \n");
            printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
        }
        #endif

        return insert_buffer(-1, key, timestamp, expiryTime, updateSeg);
    }

    #if defined DEBUG 
    printf("[Debug Info:] Append type insertion - number of gaps  does exceed threshold - append to dp \n");
    printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
    #endif

    #if defined(DEBUG_KEY) || defined(DEBUG_TS)
    if(DEBUG_KEY == key)
    {
        printf("[Debug Info:] Append type insertion - number of gaps  does exceed threshold - append to dp \n");
        printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
    }
    #endif

    //Append and copy last key to every position before insertion pos
    pair<Type_Key,Type_Ts> lastKeyForGaps = make_pair(m_localData.back().first,0);

    for (int index = m_numPair; index <= insertionPos; ++index)
    {
        m_localData.push_back(lastKeyForGaps);
    }
    m_localData.back().first = key;
    m_localData.back().second = timestamp;

    m_numPair = m_localData.size();
    ++m_numPairExist;
}

template <class Type_Key, class Type_Ts>
void SWseg<Type_Key,Type_Ts>::insert_buffer(   int insertionPos, Type_Key key, Type_Ts timestamp, Type_Ts expiryTime,
                                                vector<tuple<pswix::seg_update_type,int,Type_Key>> & updateSeg)
{
    
    //Undefined insertion position, search for insertion position
    if (insertionPos == -1) {binary_search_lower_bound_buffer(key, insertionPos);}
    
    //If buffer is not empty
    if (m_numPairBuffer && insertionPos != m_numPairBuffer)
    {   
        if (index_exists_buffer(insertionPos, expiryTime))
        {
            //Insert type insertion
            #if defined DEBUG 
            printf("[Debug Info:] Buffer insertion - insert \n");
            #endif 

            #if defined(DEBUG_KEY) || defined(DEBUG_TS)
            if(DEBUG_KEY == key)
            {
                printf("[Debug Info:] Buffer insertion - insert \n");
            }
            #endif
            
            //If previous position is empty, then insert to previous position
            if (insertionPos && !index_exists_buffer(insertionPos-1, expiryTime))
            {
                //Directly replace keys with insertion key
                m_buffer[insertionPos-1].first = key;
                m_buffer[insertionPos-1].second = timestamp;

            }
            else //Shift Buffer
            {
                m_buffer.insert(m_buffer.begin()+insertionPos, make_pair(key,timestamp));
                ++m_numPairBuffer;
            }
        }
        //Gap in buffer
        else
        {
            #if defined DEBUG 
            printf("[Debug Info:] Buffer insertion - insertionPos is a gap \n");
            printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
            // printf("[Debug Info:] key at bufferPos = %u \n", m_buffer[insertionPos].first);
            #endif

            #if defined(DEBUG_KEY) || defined(DEBUG_TS)
            if(DEBUG_KEY == key)
            {
                printf("[Debug Info:] Buffer insertion - insertionPos is a gap \n");
                printf("[Debug Info:] InsertionPos = %i \n", insertionPos );
                // printf("[Debug Info:] key at bufferPos = %u \n", m_buffer[insertionPos].first);
            }
            #endif
            
            //Directly replace keys with insertion key
            m_buffer[insertionPos].first = key;
            m_buffer[insertionPos].second = timestamp;
        }
    }
    else
    {
        #if defined DEBUG 
        printf("[Debug Info:] Buffer empty or insertion pos is end, push back \n");
        #endif

        #if defined(DEBUG_KEY) || defined(DEBUG_TS)
        if(DEBUG_KEY == key)
        {
            printf("[Debug Info:] Buffer empty or insertion pos is end, push back \n");
        }
        #endif
          
        m_buffer.push_back(make_pair(key,timestamp));
        ++m_numPairBuffer;
    }

    if (m_numPairBuffer >= MAX_BUFFER_SIZE-1)
    {
        updateSeg.push_back(make_tuple(pswix::seg_update_type::RETRAIN,m_parentIndex,m_currentNodeStartKey));
    }
}

/*
Utils Functions
*/
template<class Type_Key, class Type_Ts>
inline tuple<int,int,int> SWseg<Type_Key,Type_Ts>::find_predict_pos_bound(Type_Key targetKey)
{
    int predictPos = static_cast<int>(floor(m_slope * ((double)targetKey - (double)m_startKey)));
    int predictPosMin = predictPos - m_leftSearchBound;
    int predictPosMax = (m_rightSearchBound == 0) ? predictPos + 1 : predictPos + m_rightSearchBound;

    return (make_tuple(predictPos, predictPosMin < 0 ? 0 : predictPosMin, predictPosMax > m_numPair-1 ? m_numPair-1: predictPosMax));
}

template<class Type_Key, class Type_Ts>
inline void SWseg<Type_Key,Type_Ts>::exponential_search_dp_right(Type_Key targetKey, int & foundPos, int maxSearchBound)
{
    #ifdef TUNE
    int predictPos = foundPos;
    #endif

    int index = 1;
    while (index <= maxSearchBound && m_localData[foundPos + index].first < targetKey)
    {
        index *= 2;
    }
    
    auto startIt = m_localData.begin() + (foundPos + static_cast<int>(index/2));
    auto endIt = m_localData.begin() + (foundPos + min(index,maxSearchBound));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey,
                        [](const pair<Type_Key,Type_Ts>& data, Type_Key value)
                        {
                            return data.first < value;
                        });
    
    if (lowerBoundIt->first < targetKey && lowerBoundIt->second)
    {
        lowerBoundIt++;
    }

    foundPos = lowerBoundIt - m_localData.begin();

    #ifdef TUNE
    if (m_tuneStage == tuneStage)
    {
        segNoExponentialSearch++;
        segLengthExponentialSearch += foundPos - predictPos;
    }
    segNoExponentialSearchAll++;
    segLengthExponentialSearchAll += foundPos - predictPos;
    #endif
}

template<class Type_Key, class Type_Ts>
inline void SWseg<Type_Key,Type_Ts>::exponential_search_dp_left(Type_Key targetKey, int & foundPos, int maxSearchBound)
{
    #ifdef TUNE
    int predictPos = foundPos;
    #endif

    int index = 1;
    while (index <= maxSearchBound && m_localData[foundPos - index].first >= targetKey)
    {
        index *= 2;
    }
    
    auto startIt = m_localData.begin() + (foundPos - min(index,maxSearchBound));
    auto endIt = m_localData.begin() + (foundPos - static_cast<int>(index/2));

    auto lowerBoundIt = lower_bound(startIt,endIt,targetKey,
                        [](const pair<Type_Key,Type_Ts>& data, Type_Key value)
                        {
                            return data.first < value;
                        });
    
    if (lowerBoundIt->first < targetKey && lowerBoundIt->second)
    {
        lowerBoundIt++;
    }

    foundPos = lowerBoundIt - m_localData.begin();

    #ifdef TUNE
    if (m_tuneStage == tuneStage)
    {
        segNoExponentialSearch++;
        segLengthExponentialSearch += predictPos-foundPos;
    }
    segNoExponentialSearchAll++;
    segLengthExponentialSearchAll += predictPos-foundPos;
    #endif
}

template<class Type_Key, class Type_Ts>
inline void SWseg<Type_Key,Type_Ts>::binary_search_lower_bound_buffer(Type_Key targetKey, int & foundPos)
{
    auto lowerBoundIt = lower_bound(m_buffer.begin(),m_buffer.end(),targetKey,
                        [](const pair<Type_Key,Type_Ts>& data, Type_Key value)
                        {
                            return data.first < value;
                        });

    foundPos = lowerBoundIt - m_buffer.begin();
}


template <class Type_Key, class Type_Ts>
inline bool SWseg<Type_Key,Type_Ts>::index_exists_model(int index, Type_Ts expiryTime)
{   
    if(m_localData[index].second)
    {
        if (m_localData[index].second < expiryTime)
        {
            m_localData[index].second = 0 ;
            --m_numPairExist;
            return false;
        }
        return true;
    }
    return false;
}

template <class Type_Key, class Type_Ts>
inline bool SWseg<Type_Key,Type_Ts>::index_exists_buffer(int index, Type_Ts expiryTime)
{   
    if(m_buffer[index].second)
    {
        if(m_buffer[index].second < expiryTime)
        {
            m_buffer[index].second = 0;
            return false;
        }
        return true;
    }
    return false;
}

template <class Type_Key, class Type_Ts>
inline void SWseg<Type_Key,Type_Ts>::update_maximium_timestamp(Type_Ts timestamp)
{
    if (m_maxTimeStamp < timestamp)
    {
        m_maxTimeStamp = timestamp;
    }
}

template <class Type_Key, class Type_Ts>
inline void SWseg<Type_Key,Type_Ts>::update_maximium_search_bound()
{
    m_maxSearchError = min(8192,(int)ceil(0.6*m_numPair));
}

template <class Type_Key, class Type_Ts>
inline void SWseg<Type_Key,Type_Ts>::update_neighour_siblings(int threadLeftBoundary, int threadRightBoundary)
{
    bool leftInsertFlag = ((m_parentIndex > threadLeftBoundary) && m_leftSibling);
    bool rightInsertFlag = ((m_parentIndex < threadRightBoundary) && m_rightSibling);

    if (leftInsertFlag) //Update Left Sibling Pointer
    {
        if (rightInsertFlag)
        {
            m_leftSibling->m_rightSibling = m_rightSibling;
        }
        else
        {
            m_leftSibling->m_rightSibling = nullptr;
        }
    }
    
    if (rightInsertFlag) //Update Right Sibling Pointer
    {
        if (leftInsertFlag)
        {
            m_rightSibling->m_leftSibling = m_leftSibling;
        }
        else
        {
            m_rightSibling->m_leftSibling = nullptr;
        }
    }
}

/*
Print, Getters & Setters
*/
template <class Type_Key, class Type_Ts>
inline void SWseg<Type_Key,Type_Ts>::print()
{
    printf("start key: %u, slope %g \n", m_currentNodeStartKey, m_slope);
    if (m_numPair)
    {
        printf("data: \n");
        printf("%u(%u)", m_localData[0].first, m_localData[0].second);
        for (int i = 1; i < m_numPair; i++)
        {
            printf(", ");
            printf("%u(%u)", m_localData[i].first, m_localData[i].second);
        }
        printf("\n");
    }

    if (m_numPairBuffer)
    {
        printf("buffer: \n");
        printf("%u(%u)", m_buffer[0].first, m_buffer[0].second);
        for (int i = 1; i < m_numPairBuffer; i++)
        {
            printf(", ");
            printf("%u(%u)", m_buffer[i].first, m_buffer[i].second);
        }
        printf("\n");
    }
}

template <class Type_Key, class Type_Ts>
inline uint64_t SWseg<Type_Key,Type_Ts>::memory_usage()
{
    return sizeof(int)*7 + sizeof(Type_Ts) + sizeof(double) + sizeof(Type_Key)*2 + sizeof(vector<pair<Type_Key,Type_Ts>>)*2 +
    sizeof(pair<Type_Key,Type_Ts>)*(m_numPairBuffer + m_numPair) + sizeof(SWseg<Type_Key,Type_Ts>*)*2;
}

template <class Type_Key, class Type_Ts>
inline uint64_t SWseg<Type_Key,Type_Ts>::get_no_keys(Type_Ts expiryTime)
{
    uint64_t cnt = 0;
    if (m_numPair)
    {
        for (int i = 0; i < m_localData.size(); ++i)
        {
            if (m_localData[i].second >= expiryTime)
            {
                ++cnt;
            }
        }
    }

    if (m_numPairBuffer)
    {
        for (int i = 0; i < m_buffer.size(); ++i)
        {
            if (m_buffer[i].second >= expiryTime)
            {
                ++cnt;
            }
        }
    }

    return cnt;
}

}
#endif