#ifndef __CONCURRENT_DATA_LOADER_HPP__
#define __CONCURRENT_DATA_LOADER_HPP__

#pragma once
#include <string>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <fstream>
#include <random>
#include <algorithm>
#include <cassert>

#include "../parameters_p.hpp"

using namespace std;

/*
Dataset
*/
vector<tuple<uint64_t,uint64_t,uint64_t>> benchmark_data;

/*Util functions*/
bool sort_based_on_second(const pair<uint64_t,uint64_t> &a,const pair<uint64_t,uint64_t> &b){ return a.second<b.second;}
bool sort_based_on_first(const pair<uint64_t,uint64_t> &a,const pair<uint64_t,uint64_t> &b){ return a.first<b.first;}
bool sort_based_on_second_tuple(const tuple<uint64_t,uint64_t,uint64_t> &a,const tuple<uint64_t,uint64_t,uint64_t> &b){ return get<1>(a)<get<1>(b);}

/*Loading SOSD datasets*/
template<typename K>
bool sosd_load_binary(string filename, vector<K> &v)
{
    ifstream ifs(filename, ios::in | ios::binary);
    assert(ifs);

    K size;
    ifs.read(reinterpret_cast<char*>(&size), sizeof(K));
    v.resize(size);
    ifs.read(reinterpret_cast<char*>(v.data()), size * sizeof(K));
    ifs.close();

    return ifs.good();
}

template<typename Type_Key, typename Type_Ts>
int sosd_point_lookup(string filename)
{
    vector<Type_Key> data_binary;
    sosd_load_binary<Type_Key>(filename, data_binary);

	sort(data_binary.begin(), data_binary.end());
	data_binary.erase( unique( data_binary.begin(), data_binary.end() ), data_binary.end() );

    // std::random_device rd;
    // std::mt19937 gen(rd());
    std::mt19937 gen(SEED);
    std::uniform_int_distribution<Type_Ts> distr(1, MAX_TIMESTAMP);

    benchmark_data.reserve(TEST_LEN);
    for(auto it = data_binary.begin(); it != data_binary.begin() + TEST_LEN; ++it)
    {
        benchmark_data.push_back(make_tuple(*it, distr(gen),0));
    }
    sort(benchmark_data.begin(),benchmark_data.end(),sort_based_on_second_tuple);

    return 0;
}

template<typename Type_Key, typename Type_Ts>
int sosd_range_query(string filename)
{
    vector<Type_Key> data_binary;
    sosd_load_binary<Type_Key>(filename, data_binary);

	sort(data_binary.begin(), data_binary.end());
	data_binary.erase( unique( data_binary.begin(), data_binary.end() ), data_binary.end() );

    // std::random_device rd;
    // std::mt19937 gen(rd());
    std::mt19937 gen(SEED);
    std::uniform_int_distribution<Type_Ts> distr(1, MAX_TIMESTAMP);

    benchmark_data.reserve(TEST_LEN);
    for (int i = 0; i < TEST_LEN; i++)
    {
        if (i + MATCH_RATE > TEST_LEN)
        {
            benchmark_data.push_back(make_tuple(data_binary[i],distr(gen),data_binary[TEST_LEN-1]));
        }
        else
        {
            benchmark_data.push_back(make_tuple(data_binary[i],distr(gen),data_binary[i + MATCH_RATE -1]));
        }
    }
    sort(benchmark_data.begin(),benchmark_data.end(),sort_based_on_second_tuple);

    return 0;
}

template<typename Type_Key, typename Type_Ts>
int sosd_range_query_sequential(string filename)
{
    vector<Type_Key> data_binary;
    sosd_load_binary<Type_Key>(filename, data_binary);

	sort(data_binary.begin(), data_binary.end());
	data_binary.erase( unique( data_binary.begin(), data_binary.end() ), data_binary.end() );

    // std::random_device rd;
    // std::mt19937 gen(rd());
    std::mt19937 gen(SEED);
    std::uniform_int_distribution<Type_Ts> distr(1, MAX_TIMESTAMP);

    vector<Type_Ts> timestamps(TEST_LEN);
    for (int i = 0; i < TEST_LEN; i++)
    {
        timestamps[i] = distr(gen);
    }
    sort(timestamps.begin(),timestamps.end());

    benchmark_data.reserve(TEST_LEN);
    for (int i = 0; i < TEST_LEN; i++)
    {
        if (i + MATCH_RATE > TEST_LEN)
        {
            benchmark_data.push_back(make_tuple(data_binary[i],timestamps[i],data_binary[TEST_LEN-1]));
        }
        else
        {
            benchmark_data.push_back(make_tuple(data_binary[i],timestamps[i],data_binary[i + MATCH_RATE -1]));
        }
    }

    return 0;
}

template<typename Type_Key, typename Type_Ts>
int sosd_range_query_skewed(string filename, int no_std)
{
    //Using Normal Distribution
    vector<Type_Key> data_binary;
    sosd_load_binary<Type_Key>(filename, data_binary);

	sort(data_binary.begin(), data_binary.end());
	data_binary.erase( unique( data_binary.begin(), data_binary.end() ), data_binary.end() );

    // std::random_device rd;
    // std::mt19937 gen(rd());
    std::mt19937 gen(SEED);
    std::normal_distribution<double> distribution((1+MAX_TIMESTAMP)/2, (MAX_TIMESTAMP-1)/ (2*no_std));

    benchmark_data.reserve(TEST_LEN);
    for (int i = 0; i < TEST_LEN; i++)
    {
        double timestamp = -1;
        do
        {
            timestamp = distribution(gen);
        } while (timestamp < 1 || timestamp > MAX_TIMESTAMP);

        if (i + MATCH_RATE > TEST_LEN)
        {
            benchmark_data.push_back(make_tuple(data_binary[i],static_cast<Type_Ts>(round(timestamp)),data_binary[TEST_LEN-1]));
        }
        else
        {
            benchmark_data.push_back(make_tuple(data_binary[i],static_cast<Type_Ts>(round(timestamp)),data_binary[i + MATCH_RATE -1]));
        }
    }
    sort(benchmark_data.begin(),benchmark_data.end(),sort_based_on_second_tuple);

    return 0;
}

#endif