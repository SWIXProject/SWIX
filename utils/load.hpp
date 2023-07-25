#ifndef __DATA_LOADER_HPP__
#define __DATA_LOADER_HPP__

#pragma once
#include <string>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <fstream>
#include <random>
#include <algorithm>

#include "../parameters.hpp"

using namespace std;

bool sort_based_on_second(const pair<uint64_t,uint64_t> &a,const pair<uint64_t,uint64_t> &b)
{
       return a.second<b.second;
}

bool sort_based_on_second_tuple(const tuple<uint64_t,uint64_t,uint64_t> &a,const tuple<uint64_t,uint64_t,uint64_t> &b)
{
       return get<1>(a)<get<1>(b);
}

template<typename T>
void load_binary(string filename, vector<T> &v)
{
    ifstream ifs(filename, ios::in | ios::binary);

    if(!ifs.is_open())
    {
        cout << "Fail to open " << filename << endl;
    }

    T value;
    T size;

    ifs.read(reinterpret_cast<char*>(&size), sizeof(T));
    v.resize(size);

    ifs.read(reinterpret_cast<char*>(v.data()), size * sizeof(T));

    ifs.close();
}


template<typename Type_Key, typename Type_Ts>
int add_timestamp(string filename,  vector<pair<Type_Key, Type_Ts>> & data_t, int seed)
{

    vector<Type_Key> data;
    load_binary<Type_Key>(filename, data);

	sort(data.begin(), data.end());
	data.erase( unique( data.begin(), data.end() ), data.end() );

    // std::random_device rd;
    // std::mt19937 gen(rd());
    std::mt19937 gen(seed);
    std::uniform_int_distribution<Type_Ts> distr(1, MAX_TIMESTAMP);

    // cout << data.size() << endl;

    for(auto it = data.begin(); it != data.begin() + TEST_LEN; it++)
    {
        data_t.push_back(make_pair(*it, distr(gen)));
        // fout << *it << " " << distr(gen) << endl;
    }

    sort(data_t.begin(),data_t.end(),sort_based_on_second);

    return 0;
}


template<typename Type_Key, typename Type_Ts>
int add_timestamp(string filename, vector<tuple<Type_Key,Type_Ts,Type_Key>> & data_t, int matchrate, int seed)
{

    vector<Type_Key> data;
    load_binary<Type_Key>(filename, data);

	sort(data.begin(), data.end());
	data.erase( unique( data.begin(), data.end() ), data.end() );

    // std::random_device rd;
    // std::mt19937 gen(rd());
    std::mt19937 gen(seed);
    std::uniform_int_distribution<Type_Ts> distr(1, MAX_TIMESTAMP);

    // cout << data.size() << endl;
    for (int i = 0; i < TEST_LEN; i++)
    {
        if (i + matchrate > TEST_LEN)
        {
            data_t.push_back(make_tuple(data[i],distr(gen),data[TEST_LEN-1]));
        }
        else
        {
            data_t.push_back(make_tuple(data[i],distr(gen),data[i + matchrate -1]));
        }
    }

    sort(data_t.begin(),data_t.end(),sort_based_on_second_tuple);

    return 0;
}

template<typename Type_Key, typename Type_Ts>
int add_timestamp_squential(string filename, vector<tuple<Type_Key,Type_Ts,Type_Key>> & data_t, int matchrate, int seed)
{

    vector<Type_Key> data;
    load_binary<Type_Key>(filename, data);

	std::mt19937 gen(seed);
    std::uniform_int_distribution<Type_Ts> distr(1, MAX_TIMESTAMP);

    vector<tuple<Type_Key,Type_Ts,Type_Key>> temp;
    // cout << data.size() << endl;
    for (int i = 0; i < TEST_LEN; i++)
    {
        if (i + matchrate > TEST_LEN)
        {
            temp.push_back(make_tuple(data[i],distr(gen),data[TEST_LEN-1]));
        }
        else
        {
            temp.push_back(make_tuple(data[i],distr(gen),data[i + matchrate -1]));
        }
    }

    sort(temp.begin(),temp.end(),sort_based_on_second_tuple);

    int i = 1;
    for (auto &it: temp)
    {
        data_t.push_back(make_tuple(get<0>(it),i,get<2>(it)));
        i++;
    }

    return 0;
}


template<typename Type_Key, typename Type_Ts>
int add_timestamp_append(string filename, vector<tuple<Type_Key,Type_Ts,Type_Key>> & data_t, int matchrate, int seed)
{
    vector<Type_Key> data;
    load_binary<Type_Key>(filename, data);

	sort(data.begin(), data.end());
	data.erase( unique( data.begin(), data.end() ), data.end() );

    // std::random_device rd;
    // std::mt19937 gen(rd());
    std::mt19937 gen(seed);
    std::uniform_int_distribution<Type_Ts> distr(1, MAX_TIMESTAMP);

    vector<Type_Ts> timestamps(TEST_LEN);
    for (int i = 0; i < TEST_LEN; i++)
    {
        timestamps[i] = distr(gen);
    }
    sort(timestamps.begin(),timestamps.end());

    // cout << data.size() << endl;
    for (int i = 0; i < TEST_LEN; i++)
    {
        if (i + matchrate > TEST_LEN)
        {
            data_t.push_back(make_tuple(data[i],timestamps[i],data[TEST_LEN-1]));
        }
        else
        {
            data_t.push_back(make_tuple(data[i],timestamps[i],data[i + matchrate -1]));
        }
    }

    return 0;
}

template<typename Type_Key, typename Type_Ts>
void find_join_radius_based_on_range(vector<pair<Type_Key, Type_Ts>> data, Type_Key & joinRadius)
{
    sort(data.begin(), data.end());

    double sumJoinRadius = 0;
    for (int i = MATCH_RATE ; i < data.size(); i++)
    {  
        sumJoinRadius += data[i].first - data[i-MATCH_RATE].first;
    }

    joinRadius = static_cast<Type_Key>(sumJoinRadius/ (data.size()-MATCH_RATE));
}


template<typename Type_Key, typename Type_Ts>
int add_timestamp_partition(string filename, vector<tuple<Type_Key,Type_Ts,Type_Key>> & data_t, int noPartitions, int seed)
{
    //load data
    vector<Type_Key> data;
    load_binary<Type_Key>(filename, data);
	sort(data.begin(), data.end());
	data.erase( unique( data.begin(), data.end() ), data.end());

    vector<pair<Type_Key,Type_Key>> dataMatchRate;
    dataMatchRate.reserve(TEST_LEN);
    for (int i = 0; i < TEST_LEN; i++)
    {
        if (i + MATCH_RATE > TEST_LEN)
        {
            dataMatchRate.push_back(make_pair(data[i],data[TEST_LEN-1]));
        }
        else
        {
            dataMatchRate.push_back(make_pair(data[i],data[i + MATCH_RATE -1]));
        }
    }

    // std::random_device rd;
    // std::mt19937 gen(rd());
    // auto rng = std::default_random_engine{rd()};
    std::mt19937 gen(seed);
    auto rng = std::default_random_engine{seed};
    
    //Generate Timestamp
    vector<Type_Ts> timestamps(TEST_LEN);
    // iota(timestamps.begin(),timestamps.end(),0); //Sequential timestamp (one data expiry, another goes in)

    std::uniform_int_distribution<Type_Ts> distr(1, MAX_TIMESTAMP); //Random timestamp (still in order)
    for (int i = 0; i < TEST_LEN; i++)
    {
        timestamps[i] = distr(gen);
    }
    sort(timestamps.begin(), timestamps.end());
    
    //Shuffle Keys in each partition
    int noDataPerPartition = TEST_LEN/noPartitions;
    for (int i = 0; i < noPartitions; i++)
    {
        vector<pair<Type_Key,Type_Key>> temp(dataMatchRate.begin()+noDataPerPartition*i,dataMatchRate.begin()+(noDataPerPartition*(i+1)));
        shuffle(temp.begin(),temp.end(),rng);

        int ii = noDataPerPartition*i;
        for (auto &it: temp)
        {
            data_t.push_back(make_tuple(it.first,timestamps[ii],it.second));
            ii++;
        }
    }
    return 0;
}

template<typename Type_Key, typename Type_Ts>
int add_timestamp_multiple_data(vector<string> filenames, vector<tuple<Type_Key,Type_Ts,Type_Key>> & data_t, int noDataEachFile, int seed)
{    
    // std::random_device rd;
    // std::mt19937 gen(rd());
    std::mt19937 gen(seed);

    unordered_map<Type_Key,int> hashMap;

    Type_Ts maxTimeStamp = (TIME_WINDOW * filenames.size())*2 + 1;

    int timeStampPerFile = maxTimeStamp/filenames.size();
    
    int i = 0;
    for (auto &filename: filenames)
    {
        vector<Type_Key> data;
        load_binary<Type_Key>(filename, data);
        sort(data.begin(), data.end());
        data.erase( unique( data.begin(), data.end() ), data.end());

        std::uniform_int_distribution<Type_Ts> distr(1+timeStampPerFile*i, 1+timeStampPerFile*(i+1));

        for (int ii = 0; ii < noDataEachFile; ii++)
        {
            if (ii + MATCH_RATE > noDataEachFile)
            {
                if (hashMap.find(data[ii]) == hashMap.end())
                {
                    data_t.push_back(make_tuple(data[ii],distr(gen),data[noDataEachFile-1]));
                    hashMap.insert(make_pair(data[ii],1));
                }
            }
            else
            {
                if (hashMap.find(data[ii]) == hashMap.end())
                {
                    data_t.push_back(make_tuple(data[ii],distr(gen),data[ii + noDataEachFile -1]));
                    hashMap.insert(make_pair(data[ii],1));
                }
            }
        }
        i++;
    }
    sort(data_t.begin(),data_t.end(),sort_based_on_second_tuple);
    // data_t.erase(   unique(data_t.begin(), data_t.end(),
    //                 [](const tuple<Type_Key,Type_Ts,Type_Key>&a, const tuple<Type_Key,Type_Ts,Type_Key> & b)
    //                 {
    //                     return get<0>(a) == get<0>(b);
    //                 })
    //             , data_t.end());
    return 0;
}

template<typename Type_Key, typename Type_Ts>
int add_timestamp_skewness(string filename, vector<tuple<Type_Key,Type_Ts,Type_Key>> & data_t, int no_std, int seed)
{
    //Using Normal Distribution
    vector<Type_Key> data;
    load_binary<Type_Key>(filename, data);

	sort(data.begin(), data.end());
	data.erase( unique( data.begin(), data.end() ), data.end() );

    // std::random_device rd;
    // std::mt19937 gen(rd());
    std::mt19937 gen(seed);
    std::normal_distribution<double> distribution((1+MAX_TIMESTAMP)/2, (MAX_TIMESTAMP-1)/ (2*no_std));

    // cout << data.size() << endl;
    for (int i = 0; i < TEST_LEN; i++)
    {
        double timestamp = -1;
        do
        {
            timestamp = distribution(gen);
        } while (timestamp < 1 || timestamp > MAX_TIMESTAMP);

        if (i + MATCH_RATE > TEST_LEN)
        {
            data_t.push_back(make_tuple(data[i],static_cast<Type_Ts>(round(timestamp)),data[TEST_LEN-1]));
        }
        else
        {
            data_t.push_back(make_tuple(data[i],static_cast<Type_Ts>(round(timestamp)),data[i + MATCH_RATE -1]));
        }
    }
    sort(data_t.begin(),data_t.end(),sort_based_on_second_tuple);

    return 0;
}

#endif