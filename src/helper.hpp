#ifndef __SWIX_HELPER_HPP__
#define __SWIX_HELPER_HPP__

#pragma once
#include <iostream>
#include <algorithm>
#include <iterator>
#include <vector>
#include <array>
#include <cmath>
#include <stdexcept>
#include<numeric>
#include<cassert>
#include <x86intrin.h>
#include <bitset>
#include <stdint.h>

#include "config.hpp"

using namespace std;

namespace swix {

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

#ifdef TUNE_TIME
#include "../utils/tuner_time_variables.hpp"
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

uint64_t rootNoSearch = 0;
uint64_t rootNoPredict = 0;
uint64_t rootPredictCycle = 0;
uint64_t rootExponentialSearchCycle = 0;
double rootExponentialSearchCyclePerLength = 0;

uint64_t segNoSearch = 0;
uint64_t segNoPredict = 0;
uint64_t segPredictCycle = 0;
uint64_t segExponentialSearchCycle = 0;
double segExponentialSearchCyclePerLength = 0;

uint64_t bufferNoSearch = 0;
uint64_t bufferLengthBinarySearch = 0;
uint64_t bufferBinarySearchCycle = 0;
double bufferBinarySearchCyclePerLength = 0;

uint64_t rootInsertCycle = 0;
double rootInsertCyclePerLength = 0;
uint64_t rootNoInsert = 0;
uint64_t rootSizeInsert = 0;

uint64_t segNoInsert = 0;
uint64_t segInsertCycle = 0;

uint64_t rootNoDeletion = 0;
uint64_t rootDeletionCycle = 0;

uint64_t rootNoRetrain = 0;
uint64_t rootLengthRetrain = 0;
uint64_t rootRetrainCycle = 0;
double rootRetrainCyclePerLength = 0;

uint64_t segMergeCycle = 0;
double  segMergeCyclePerLength = 0;
uint64_t segRetrainCycle = 0;
double segRetrainCyclePerLength = 0;

uint64_t searchCycle = 0;
uint64_t insertCycle = 0;
uint64_t scanCycle = 0;
uint64_t retrainCycle = 0;
#endif

#ifdef TIME_BREAKDOWN
#include "../utils/timer_variables.hpp"
uint64_t searchCycle = 0;
uint64_t insertCycle = 0;
uint64_t scanCycle = 0;
uint64_t retrainCycle = 0;
#endif

#ifdef PRINT
#include "../utils/print_stats_variables.hpp"

uint64_t print_rootNoExponentialSearch = 0;
uint64_t print_rootLengthExponentialSearch = 0;

uint64_t print_segNoExponentialSearch = 0;
uint64_t print_segLengthExponentialSearch = 0;

uint64_t print_segNoScan = 0;
uint64_t print_segScanNoSeg = 0;

uint64_t print_segNoRetrain = 0;
uint64_t print_segLengthMerge = 0;
uint64_t print_segLengthRetrain = 0;

uint64_t print_segNoExponentialSearchAll = 0;
uint64_t print_segLengthExponentialSearchAll = 0;
#endif

#if defined(DEBUG) || defined(DEBUG_KEY)
#include "../utils/debug.hpp"
#endif

/*
Function Headers
*/
template<class Type_Key, class Type_Ts>
void calculate_split_derivative(int noSeg, vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> & splitIndexSlopeVector);

template<class Type_Key, class Type_Ts>
void calculate_split_equal(int noSeg, vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> & splitIndexSlopeVector);

template<class Type_Key, class Type_Ts>
void calculate_split_index_one_pass(vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> &splitIndexSlopeVector);

template<class Type_Key, class Type_Ts>
void calculate_split_index_one_pass(vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> &splitIndexSlopeVector, int & splitError);

template<class Type_Key, class Type_Ts>
void calculate_split_index_one_pass_least_sqaure(vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> &splitIndexSlopeVector, int & splitError);

template<class Type_Key, class Type_Ts>
void calculate_split_index_one_pass(vector<Type_Key> & data, vector<tuple<int,int,double>> &splitIndexSlopeVector, int & splitError);

template<class Type_Key, class Type_Ts>
void calculate_split_derivative(int noSeg, vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> & splitIndexSlopeVector)
{
    #ifdef DEBUG
    cout << "[Debug Info:] Util Function {calculate_split_index_vect(noSeg, data, slplitIndexVector)} Begin" << endl;
    cout << "[Debug Info:] size of data = " << data.size() << endl;
    cout << endl;
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
        sumKey += (double)data[i].first;
        sumIndex += cnt;
        sumKeyIndex += (double)data[i].first * cnt;
        sumKeySquared += (double)pow(data[i].first,2);
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
    cout << "[Debug Info:] Printing slplitIndexVector : " << endl;
    for (auto &it: splitIndexSlopeVector)
    {
        cout << "start = " << get<0>(it) << ", end = " << get<1>(it) << ", slope = " << get<2>(it) << endl;
    }
    cout << endl;
    #endif

    #ifdef DEBUG
    cout << "[Debug Info:] Util Function {calculate_split_index_vect(noSeg, data, slplitIndexVector)} End" << endl;
    #endif
}

template<class Type_Key, class Type_Ts>
void calculate_split_equal(int noSeg, vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> & splitIndexSlopeVector)
{
    #ifdef DEBUG
    cout << "[Debug Info:] Util Function {calculate_split_index_vect(noSeg, data, slplitIndexVector)} Begin" << endl;
    cout << "[Debug Info:] size of data = " << data.size() << endl;
    cout << endl;
    #endif

    int segSize = data.size()/(noSeg);
    vector<bool> splitIndexVector(data.size());
    
    int startIndex = 0;
    for (int i = 0; i < noSeg-1; i++)
    {
        splitIndexVector[startIndex] = true;
        splitIndexVector[startIndex+segSize-1] = true;
        startIndex = startIndex+segSize;
    }

    if (startIndex >= data.size()-1)
    {
        splitIndexVector.back() = true;
    }
    else
    {
        splitIndexVector[startIndex] = true;
        splitIndexVector.back() = true;
    }

    int startSplitIndex = 0, cnt = 0;
    double sumKey = 0,sumIndex = 0, sumKeyIndex = 0, sumKeySquared = 0;
    for (int i = 0; i < data.size(); i++)
    {
        sumKey += (double)data[i].first;
        sumIndex += cnt;
        sumKeyIndex += (double)data[i].first * cnt;
        sumKeySquared += (double)pow(data[i].first,2);
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
    cout << "[Debug Info:] Printing slplitIndexVector : " << endl;
    for (auto &it: splitIndexSlopeVector)
    {
        cout << "start = " << get<0>(it) << ", end = " << get<1>(it) << ", slope = " << get<2>(it) << endl;
    }
    cout << endl;
    #endif

    #ifdef DEBUG
    cout << "[Debug Info:] Util Function {calculate_split_index_vect(noSeg, data, slplitIndexVector)} End" << endl;
    #endif
}


template<class Type_Key, class Type_Ts>
void calculate_split_index_one_pass(vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> &splitIndexSlopeVector)
{
    #ifdef DEBUG
    cout << "[Debug Info:] Util Function {calculate_split_index_one_pass(data, slplitIndexSlope)} Start" << endl;
    cout << "[Debug Info:] size of DualData = " << data.size() << endl;
    cout << endl;
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
            #ifdef DEBUG
            double slopeHighTemp = ((double)(index + INITIAL_ERROR)) / denominator;
 		    double slopeLowTemp = ((double)(index - INITIAL_ERROR)) / denominator;
            #endif
            
            slopeHigh = min(slopeHigh,(((double)(index + INITIAL_ERROR)) / denominator));
            slopeLow = max(slopeLow, (((double)(index - INITIAL_ERROR)) / denominator));
            slope = 0.5*(slopeHigh + slopeLow);

            index++;
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

        splitIndexEnd++;
    }

    splitIndexSlopeVector.push_back(make_tuple(splitIndexStart,splitIndexEnd-1,slope));

    #ifdef DEBUG
    cout << "[Debug Info:] Util Function {calculate_split_index_one_pass(data, slplitIndexSlope)} End" << endl;
    cout << "Shrinking Cone Split:" << endl;
    for (auto &it: splitIndexSlopeVector)
    {
        cout << "start = " << get<0>(it) << ", end = " << get<1>(it) << ", slope = " << get<2>(it) << endl;
    }
    cout <<endl;
    #endif
}

template<class Type_Key, class Type_Ts>
void calculate_split_index_one_pass(vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> &splitIndexSlopeVector, int & splitError)
{
    #ifdef DEBUG
    cout << "[Debug Info:] Util Function {calculate_split_index_one_pass(data, slplitIndexSlope, splitError)} Start" << endl;
    cout << "[Debug Info:] size of DualData = " << data.size() << endl;
    cout << endl;
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
            #ifdef DEBUG
            double slopeHighTemp = ((double)(index + INITIAL_ERROR)) / denominator;
 		    double slopeLowTemp = ((double)(index - INITIAL_ERROR)) / denominator;
            #endif
            
            slopeHigh = min(slopeHigh,(((double)(index + splitError)) / denominator));
            slopeLow = max(slopeLow, (((double)(index - splitError)) / denominator));
            slope = 0.5*(slopeHigh + slopeLow);

            index++;
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

        splitIndexEnd++;
    }

    splitIndexSlopeVector.push_back(make_tuple(splitIndexStart,splitIndexEnd-1,slope));

    #ifdef DEBUG
    cout << "[Debug Info:] Util Function {calculate_split_index_one_pass(data, slplitIndexSlope, splitError)} End" << endl;
    cout << "Shrinking Cone Split:" << endl;
    for (auto &it: splitIndexSlopeVector)
    {
        cout << "start = " << get<0>(it) << ", end = " << get<1>(it) << ", slope = " << get<2>(it) << endl;
    }
    cout <<endl;
    #endif
}


template<class Type_Key, class Type_Ts>
void calculate_split_index_one_pass_least_sqaure(vector<pair<Type_Key,Type_Ts>> & data, vector<tuple<int,int,double>> &splitIndexSlopeVector, int & splitError)
{
    #ifdef DEBUG
    cout << "[Debug Info:] Util Function {calculate_split_index_one_pass_least_sqaure(data, slplitIndexSlope, splitError)} Start" << endl;
    cout << "[Debug Info:] size of DualData = " << data.size() << endl;
    cout << endl;
    #endif

    double slopeLow = 0;
    double slopeHigh = numeric_limits<double>::max();
    double slope = 1;

    double keyStart = data.front().first;
    
    int index = 1;
    int splitIndexStart = 0; 
    int splitIndexEnd = 1;

    double sumKey = (double)keyStart,sumIndex = 1, sumKeyIndex = 0, sumKeySquared = (double)pow(keyStart,2);
    for (auto it = data.begin()+1; it != data.end(); it++)
    {
        double denominator = it->first - keyStart;

        if (slopeLow <= (double)(index) / denominator &&
		    (double)(index) / denominator <= slopeHigh) 
        {
            #ifdef DEBUG
            double slopeHighTemp = ((double)(index + INITIAL_ERROR)) / denominator;
 		    double slopeLowTemp = ((double)(index - INITIAL_ERROR)) / denominator;
            #endif
            
            slopeHigh = min(slopeHigh,(((double)(index + splitError)) / denominator));
            slopeLow = max(slopeLow, (((double)(index - splitError)) / denominator));

            sumKey += (double)it->first;
            sumIndex += index;
            sumKeyIndex += (double)it->first * index;
            sumKeySquared += (double)pow(it->first,2);
            slope = (sumKeyIndex - sumKey *(sumIndex/index))/(sumKeySquared - sumKey*(sumKey/index));

            index++;
        }
        else
        {            
            splitIndexSlopeVector.push_back(make_tuple(splitIndexStart,splitIndexEnd-1,slope));
            
            splitIndexStart = splitIndexEnd;

            index = 1;
            keyStart = it->first;
            slopeLow = 0;
            slopeHigh = numeric_limits<double>::max();

            sumKey = (double)it->first;
            sumIndex = 0;
            sumKeyIndex = 0;
            sumKeySquared = (double)pow(it->first,2);
            slope = 1;
        }

        splitIndexEnd++;
    }
    
    splitIndexSlopeVector.push_back(make_tuple(splitIndexStart,splitIndexEnd-1,slope));

    #ifdef DEBUG
    cout << "[Debug Info:] Util Function {calculate_split_index_one_pass_least_sqaure(data, slplitIndexSlope, splitError)} End" << endl;
    cout << "Shrinking Cone Split:" << endl;
    for (auto &it: splitIndexSlopeVector)
    {
        cout << "start = " << get<0>(it) << ", end = " << get<1>(it) << ", slope = " << get<2>(it) << endl;
    }
    cout <<endl;
    #endif
}

template<class Type_Key, class Type_Ts>
void calculate_split_index_one_pass(vector<Type_Key> & data, vector<tuple<int,int,double>> &splitIndexSlopeVector, int & splitError)
{
    #ifdef DEBUG
    cout << "[Debug Info:] Util Function {calculate_split_index_one_pass(data, slplitIndexSlope, splitError)} Start" << endl;
    cout << "[Debug Info:] size of DualData = " << data.size() << endl;
    cout << endl;
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
        double denominator = *it - keyStart;

        if (slopeLow <= (double)(index) / denominator &&
		    (double)(index) / denominator <= slopeHigh) 
        {
            #ifdef DEBUG
            double slopeHighTemp = ((double)(index + INITIAL_ERROR)) / denominator;
 		    double slopeLowTemp = ((double)(index - INITIAL_ERROR)) / denominator;
            #endif
            
            slopeHigh = min(slopeHigh,(((double)(index + splitError)) / denominator));
            slopeLow = max(slopeLow, (((double)(index - splitError)) / denominator));
            slope = 0.5*(slopeHigh + slopeLow);

            index++;
        }
        else
        {
            splitIndexSlopeVector.push_back(make_tuple(splitIndexStart,splitIndexEnd-1,slope));
            
            splitIndexStart = splitIndexEnd;
            index = 1;
            keyStart = *it;
            slopeLow = 0;
            slopeHigh = numeric_limits<double>::max();
            slope = 1;
        }

        splitIndexEnd++;
    }

    splitIndexSlopeVector.push_back(make_tuple(splitIndexStart,splitIndexEnd-1,slope));

    #ifdef DEBUG
    cout << "[Debug Info:] Util Function {calculate_split_index_one_pass(data, slplitIndexSlope, splitError)} End" << endl;
    cout << "Shrinking Cone Split:" << endl;
    for (auto &it: splitIndexSlopeVector)
    {
        cout << "start = " << get<0>(it) << ", end = " << get<1>(it) << ", slope = " << get<2>(it) << endl;
    }
    cout <<endl;
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