#ifndef __PARALLEL_SWIX_HELPER_HPP__
#define __PARALLEL_SWIX_HELPER_HPP__

#pragma once
#include<iostream>
#include<algorithm>
#include<iterator>
#include<vector>
#include<numeric>
#include<cassert>
#include<cmath>
#include<tuple>

#include <x86intrin.h>
#include <bitset>
#include <stdint.h>

#include <atomic>
#include <mutex>
#include <condition_variable>
#include "../lib/multithread_queues/concurrent_queue.h"

#include "config_p.hpp"
#include "../utils/print_util.hpp"

#if defined(DEBUG) || defined(DEBUG_KEY) || defined(DEBUG_TS)
#include "../utils/print_debug_util.hpp"
#endif

using namespace std;

namespace pswix {

#ifdef TUNE
#include "../utils/tuner_variables.hpp"
uint64_t tuneStage = 0;

uint64_t rootNoExponentialSearch = 0;
uint64_t rootLengthExponentialSearch = 0;

uint64_t segNoExponentialSearch = 0;
uint64_t segLengthExponentialSearch = 0;

uint64_t segNoScan = 0;
uint64_t segScanNoSeg = 0;

uint64_t segNoRetrain = 0;
uint64_t segLengthMerge = 0;
uint64_t segLengthRetrain = 0;

uint64_t segNoExponentialSearchAll = 0;
uint64_t segLengthExponentialSearchAll = 0;
#endif

/*
Helper decalrations
*/

enum class seg_update_type {NONE = 0, RETRAIN = 1, REPLACE = 2, DELETE = 3};

/*
Function Headers
*/
template<class Type_Key, class Type_Ts>
void calculate_split_derivative(int noSeg, const vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> & splitIndexSlopeVector);

template<class Type_Key, class Type_Ts>
void calculate_split_index_one_pass(const vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> &splitIndexSlopeVector, int splitError);

template<class Type_Key, class Type_Ts>
void calculate_split_index_one_pass_least_sqaure(const vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> &splitIndexSlopeVector, int splitError);


/*
Functions
*/
template<class Type_Key, class Type_Ts>
void calculate_split_derivative(int noSeg, const vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> & splitIndexSlopeVector)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("Util","calculate_split_derivative");
    #endif

    vector<double> secondDerivative(data.size());
    secondDerivative[0] = 0;
    secondDerivative.back() = 0;
    for (int i = 2; i < data.size(); i++)
    {
        secondDerivative[i-1] =  abs((double)(data[i].first-data[i-1].first) - (data[i-1].first-data[i-2].first));      
    }

    vector<int> indexVec(data.size());
    iota(indexVec.begin(),indexVec.end(),0);
    sort(indexVec.begin(),indexVec.end(), [&](double i,double j){return secondDerivative[i] > secondDerivative[j];} );

    int slpitTimes = 0;
    vector<bool> splitIndexVector(data.size());
    splitIndexVector.back() = true;

    int halfSegSize = data.size()/(noSeg*2);

    for(int i = 0; i < indexVec.size(); i++)
    {        
        if(slpitTimes >= noSeg-1 || secondDerivative[indexVec[i]] == 0)
        {
            break;
        }

        if( secondDerivative[indexVec[i]] > 0)
        {
            if (secondDerivative[indexVec[i+1]] < secondDerivative[indexVec[i-1]])
            {
                splitIndexVector[indexVec[i-1]] = true;
            }
            else
            {
                splitIndexVector[indexVec[i]] = true;
            }

            int tempIndex = indexVec[i]-1;
            while (tempIndex >= 0 && tempIndex >= indexVec[i]-halfSegSize)
            {
                secondDerivative[tempIndex--] = -1;
            }

            tempIndex = indexVec[i]+1;
            while (tempIndex < data.size() && tempIndex <= indexVec[i]+halfSegSize)
            {
                secondDerivative[tempIndex++] = -1;
            }

            slpitTimes++;
        
        }
    }

    int startSplitIndex = 0, cnt = 0;
    double sumKey = 0,sumIndex = 0, sumKeyIndex = 0, sumKeySquared = 0;
    for (int i = 0; i < data.size(); i++)
    {
        double nomalizedKey = (double)data[i].first - (double)data[0].first;
        sumKey += nomalizedKey;
        sumIndex += cnt;
        sumKeyIndex += nomalizedKey * cnt;
        sumKeySquared += pow(nomalizedKey,2);
        cnt++;
        
        if(splitIndexVector[i])
        {
            splitIndexSlopeVector.push_back(make_tuple(startSplitIndex,i,(sumKeyIndex - sumKey *(sumIndex/cnt))/(sumKeySquared - sumKey*(sumKey/cnt))));

            sumKey = 0;
            sumIndex = 0;
            sumKeyIndex = 0;
            sumKeySquared = 0;
            cnt = 0;
            startSplitIndex = i+1;
        }
    }

    #ifdef DEBUG
    printf("[Debug Info:] Printing split points : \n");
    for (auto &it: splitIndexSlopeVector)
    {
        printf("start = %i, end = %i, slope = %g \n", get<0>(it), get<1>(it), get<2>(it));
    }

    DEBUG_EXIT_FUNCTION("Util","calculate_split_derivative");
    #endif
}


template<class Type_Key, class Type_Ts>
void calculate_split_index_one_pass(const vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> &splitIndexSlopeVector, int & splitError)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("Util","calculate_split_index_one_pass");
    printf("[Debug Info:] size of data = %i \n", data.size());
    #endif

    double slopeLow = 0;
    double slopeHigh = numeric_limits<double>::max();
    double slope = 1;

    double keyStart = data.front().first;
    
    int index = 1;
    int splitIndexStart = 0; 
    int splitIndexEnd = 1;

    for (auto it = data.begin()+1; it != data.end(); it++)
    {
        double denominator = it->first - keyStart;

        if (slopeLow <= (double)(index) / denominator &&
		    (double)(index) / denominator <= slopeHigh) 
        {
            slopeHigh = min(slopeHigh,(((double)(index + splitError)) / denominator));
            slopeLow = max(slopeLow, (((double)(index - splitError)) / denominator));
            slope = 0.5*(slopeHigh + slopeLow);

            ++index;
        }
        else
        {
            splitIndexSlopeVector.push_back(make_tuple(splitIndexStart,splitIndexEnd-1,slope));
            
            splitIndexStart = splitIndexEnd;
            index = 1;
            keyStart = it->first;
            slopeLow = 0;
            slopeHigh = numeric_limits<double>::max();
            slope = 1;
        }

        ++splitIndexEnd;
    }

    splitIndexSlopeVector.push_back(make_tuple(splitIndexStart,splitIndexEnd-1,slope));
    
    #ifdef DEBUG
    printf("[Debug Info:] Printing split points : \n");
    for (auto &it: splitIndexSlopeVector)
    {
        printf("start = %i, end = %i, slope = %g \n", get<0>(it), get<1>(it), get<2>(it));
    }

    DEBUG_EXIT_FUNCTION("Util","calculate_split_index_one_pass");
    #endif
}

template<class Type_Key, class Type_Ts>
void calculate_split_index_one_pass_least_sqaure(const vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> &splitIndexSlopeVector, int splitError)
{
    #ifdef DEBUG
    DEBUG_ENTER_FUNCTION("Util","calculate_split_index_one_pass_least_sqaure");
    printf("[Debug Info:] size of data = %i \n", data.size());
    #endif

    double slopeLow = 0;
    double slopeHigh = numeric_limits<double>::max();
    double slope = 1;

    double keyStart = data.front().first;
    
    int index = 1;
    int splitIndexStart = 0; 
    int splitIndexEnd = 1;

    double sumKey = 0, sumIndex = 0, sumKeyIndex = 0, sumKeySquared = 0;
    for (auto it = data.begin()+1; it != data.end(); it++)
    {
        double denominator = it->first - keyStart;
        double nomalizedKey = (double)it->first - keyStart;

        if (slopeLow <= (double)(index) / denominator &&
		    (double)(index) / denominator <= slopeHigh) 
        {
            slopeHigh = min(slopeHigh,(((double)(index + splitError)) / denominator));
            slopeLow = max(slopeLow, (((double)(index - splitError)) / denominator));

            sumKey += nomalizedKey;
            sumIndex += index;
            sumKeyIndex += nomalizedKey * index;
            sumKeySquared += (double)pow(nomalizedKey,2);
            slope = (sumKeyIndex - sumKey *(sumIndex/index))/(sumKeySquared - sumKey*(sumKey/index));

            ++index;
        }
        else
        {            
            splitIndexSlopeVector.push_back(make_tuple(splitIndexStart,splitIndexEnd-1,slope));
            
            splitIndexStart = splitIndexEnd;

            index = 1;
            keyStart = it->first;
            slopeLow = 0;
            slopeHigh = numeric_limits<double>::max();

            sumKey = 0;
            sumIndex = 0;
            sumKeyIndex = 0;
            sumKeySquared = 0;
            slope = 1;
        }

        ++splitIndexEnd;
    }
    
    splitIndexSlopeVector.push_back(make_tuple(splitIndexStart,splitIndexEnd-1,slope));

    #ifdef DEBUG
    printf("[Debug Info:] Printing split points : \n");
    for (auto &it: splitIndexSlopeVector)
    {
        printf("start = %i, end = %i, slope = %g \n", get<0>(it), get<1>(it), get<2>(it));
    }

    DEBUG_EXIT_FUNCTION("Util","calculate_split_index_one_pass_least_sqaure");
    #endif
}

/*
Bitmap Helpers
from ALEX (https://github.dev/microsoft/ALEX)
*/

//extract_rightmost_one(010100100) = 000000100  or if value = 10 (1010) -> extracts 2 (0010)
inline uint64_t bit_extract_rightmost_bit(uint64_t value)
{
    return value & -static_cast<int64_t>(value);
}

//remove_rightmost_one(010100100) = 010100000
inline uint64_t bit_remove_rightmost_one(uint64_t value) {
  return value & (value - 1);
}

//count_ones(010100100) = 3
inline int bit_count_ones(uint64_t value) {
  return static_cast<int>(_mm_popcnt_u64(value));
}

//Get index of bit in bitmap
//bitmapPos is the position of the current bitmap in the vector/array of bitmap
//value is the an uin64_t where the bit at the target position is 1 while the rest is 0. (i.e. 000000100000 if bit at index 6 is non-gap)
//value is usually a product of extract_rightmost_bit() (i.e. get_index(bitmapPos,extract_rightmost_bit(bitmap[bitmapPos])));
inline int bit_get_index(int bitmapPos, uint64_t value) {
  return (bitmapPos << 6) + bit_count_ones(value - 1);
}

}
#endif