#ifndef __PSFlirt_CONCURRENT_HPP__
#define __PSFlirt_CONCURRENT_HPP__

#pragma once
#include "rwlock.hpp"
#ifdef __FLIRT_HELPER_HPP__
#inclde "../flirt/flirt_helper.hpp"
#else
#include "flirt_helper.hpp"
#endif

#ifndef INITIAL_ERROR
#define GLOBAL_ERROR 256
#else
#define GLOBAL_ERROR INITIAL_ERROR
#endif

namespace flirt {

using namespace std;

template<class K> class PSFlirt;

/*
***************** LP Segments *****************
*/
template<class K>
class Segment
{
    int n;    //current number of keys
	int nDelete; //number of keys deleted from keys
    int startingKeyPos; // starting key position in entire queue;

	K keyStart; //starting key
    
	double slope;
	double slopeLow;  //Low Slope of Segment
	double slopeHigh; //High Slope of Segment

	vector<K> keys;   //Vector of keys
	Segment<K>* leftSibling = nullptr; //Pointer to the left sibling (leaf only)
	Segment<K>* rightSibling = nullptr; //Pointer to the right sibling (leaf only)

    SharedMutexRW segMutex; //Segment Level Read-Write Lock

public:
    //Constructor
	Segment(K keyStart, int startingKeyPos, double slopeLow = 0, double slopeHigh = numeric_limits<double>::max());

    //Lookup
    bool lookup(K & key, int & deletedPos);

    //Enqueue
	Segment<K>* push_back(K key, int startingKeyPos);

    //Getters
    size_t get_model_size_in_bytes();
    size_t get_total_size_in_bytes();

    friend class PSFlirt<K>;
};


/*
Constructor
*/
template<class K>
Segment<K>::Segment(K keyStart, int startingKeyPos, double slopeLow, double slopeHigh) 
{
    this->slopeLow = slopeLow;
    this->slopeHigh = slopeHigh;
    this->slope = 1;
    this->keyStart = keyStart;
    this->keys = { keyStart };
    this->n = 1;
    this->nDelete = 0;
    this->startingKeyPos = startingKeyPos;
}

/*
Search
*/
template<class K>
bool Segment<K>::lookup(K & key, int & deletedPos) {

    segMutex.lock_shared();

    if (n == 1) {
		if (keys[0] == key)
        {
            return (startingKeyPos > deletedPos);
        }
        else
        {
            return false;
        }
	}
	
	int pos = (key - keyStart) * slope;
    bool results = binary_search_vector(keys, pos - GLOBAL_ERROR, pos + GLOBAL_ERROR, key, startingKeyPos, deletedPos);

    segMutex.unlock_shared();

    return results;
}


/*
Push Back
*/
template<class K>
Segment<K>* Segment<K>::push_back(K key, int startingKeyPos) {

	if (key == keys[n - 1]) { return nullptr; }
	if (key < keys[n - 1]) {
		throw invalid_argument("Key must be largest to be appended");
	}

	//If less than error than just add to leaf
	if (slopeLow <= (double)(n) / (double)(key - keyStart) &&
		(double)(n) / (double)(key - keyStart) <= slopeHigh) 
    {
		double slopeHighTemp = (double)((n + GLOBAL_ERROR)) / (double)(key - keyStart);
		double slopeLowTemp = (double)((n - GLOBAL_ERROR)) / (double)(key - keyStart);

        segMutex.lock();

		slopeHigh = min(slopeHigh, slopeHighTemp);
		slopeLow = max(slopeLow, slopeLowTemp);
		slope = (slopeHigh + slopeLow) / 2;
		keys.push_back(key);
		n++;

        segMutex.unlock();

		return nullptr;
	}
	//Else create new segment
	else 
    {
        Segment<K>* segPtr = new Segment<K>(key, startingKeyPos);
		
        segMutex.lock();
        
        rightSibling = segPtr;
		segPtr->leftSibling = this;

        segMutex.unlock();

		return segPtr;
	}
}

/*
Size of each Segment
*/
template<class K>
size_t Segment<K>::get_model_size_in_bytes()
{
    return sizeof(int)*2 + sizeof(double)*3 + sizeof(K) + sizeof(Segment<K>*)*2 + sizeof(vector<K>);
}

template<class K>
size_t Segment<K>::get_total_size_in_bytes()
{
    return sizeof(int)*2 + sizeof(double)*3 + sizeof(K) + sizeof(Segment<K>*)*2 + sizeof(vector<K>)
    + sizeof(K)*n;
}


/*
***************** global PSFlirt with locks *****************
*/

template<class K>
class PSFlirt 
{
private:
    int n; // current size of the queue
    int capacity; //capacity
    int first_index;  //front index
    int last_index;  //last index
    int keyPos; //position of key in queue

    K first_key; //First key in PSFlirt
    K last_key; //Last key in PSFlirt

    Segment<K>* first = nullptr; //first segment in queue
    Segment<K>* last = nullptr; //last segment in queue

    pair<K,Segment<K>*>* queue; //queue which is the SummaryList

    SharedMutexRW queueMutex; //Queue Level Read-Write Lock

public:
    //Constructor & Destructor
    PSFlirt(int size);
    ~PSFlirt();

    //Lookup
    bool lookup(K target, int deletePos);

private:
    //Search Helpers
    bool linear_search(K target, int left, int right, int & deletePos);
    bool binary_search(K target, int left, int right, int & deletePos);

public:
    //Enqueue
    void enqueue(K key);
    void update(K key, int deletePos);

    //Utils
    void clear();
    void printQueue();
    size_t get_model_size_in_bytes();
    size_t get_total_size_in_bytes();

};

/*
Constructor & Destructor
*/
template<class K>
PSFlirt<K>::PSFlirt(int size)
{
    queue = new pair<K,Segment<K>*>[size];
    capacity = size;
    first_index = 0;
    last_index = 0;
    n = 0;
    keyPos = 0;
}

template<class K>
PSFlirt<K>::~PSFlirt()
{
    if (n != 0)
    {
        Segment<K>* temp = first->rightSibling;
        delete first;
        while (temp != NULL)
        {
            Segment<K>* temp2 = temp->rightSibling;
            delete temp;
            temp = temp2;
        }
    }
    delete[] queue;
    queue = nullptr;
}

/*
Search
*/
template<class K>
bool PSFlirt<K>::lookup(K target, int deletePos)
{
    
    if (n == 0)
    {
        // cout << "Empty Queue" << endl;
        return false;
    }

    if (n*sizeof(K) < 256)
    {
        return linear_search(target,first_index, last_index, deletePos);
    }
    else
    {
        return binary_search(target,first_index, last_index, deletePos);
    }
}


/*
Linear Search Method used in Search Operation
For finding the segment in SummaryList
*/
template<class K>
bool PSFlirt<K>::linear_search(K target, int left, int right, int & deletePos)
{
    int index = left;
    while (index < right && queue[index+1].first <= target) {index++;}
    return queue[index].second->lookup(target, deletePos);
}

/*
Binary Search Method used in Search Operation
For finding the segment in SummaryList
*/
template<class K>
bool PSFlirt<K>::binary_search(K target, int left, int right, int & deletePos)
{
    
    //edge case (left  boundary)
    if (target < queue[left+1].first)
    {
        if (queue[left].first <= target)
        {
            return queue[left].second->lookup(target, deletePos);
        }
        else
        {
            return false;
        }
    }

    //edge case (right boundary)
    if (target >= queue[right-1].first)
    {
        if (target < queue[right].first)
        {
            return queue[right-1].second->lookup(target, deletePos);
        }
        else
        {
            return queue[right].second->lookup(target, deletePos);
        }
    }

    while(left <= right)
    {
        int mid = (left + right) / 2;
        // if (queue[mid].first <= target && queue[mid+1].first > target)
        if (queue[mid].first == target)
        {
            return queue[mid].second->lookup(target, deletePos);
        }
        if (queue[mid].first < target)
        {
            if (queue[mid-1].first >= target)
            {
                return queue[mid-1].second->lookup(target, deletePos);  
            }
            left = mid + 1;
        }
        else
        {
            if (queue[mid+1].first < target)
            {
                return queue[mid].second->lookup(target, deletePos);  
            }
            right = mid - 1;
        }
    }
    return false;
}

/*
Enqueue 
*/
template<class K>
void PSFlirt<K>::enqueue(K key)
{
    if (n == capacity)
    {
        cout << "The queue is full" << endl;
        return;
    }

    if (n == 0)
    {
        Segment<K>* segPtr = new Segment<K>(key, keyPos);
        queue[n] = make_pair(key,segPtr);
        first = segPtr;
        last = segPtr;
        n++;
        first_key = key;
        last_key = key;
        keyPos++;
        return;
    }

    Segment<K>* newSeg = last->push_back(key, keyPos);
    keyPos++;

    if (newSeg != NULL){
        last_index++;
        last_index %= capacity;
        queue[last_index] = make_pair(key,newSeg);
        n++;

        last = newSeg;
        last_key = key;
    }
    else
    {
        last_key = key;
    }
    
    return;
}

/*
Update
*/
template<class K>
void PSFlirt<K>::update(K key, int deletePos)
{
    Segment<K>* newSeg = last->push_back(key, keyPos);
    keyPos++;

    if (newSeg != NULL)
    {
        queueMutex.lock();
        
        last_index++;
        last_index %= capacity;
        queue[last_index] = make_pair(key,newSeg);
        n++;

        last = newSeg;
        last_key = key;

        if (first->rightSibling && first->rightSibling->startingKeyPos == deletePos)
        {
            first_index ++;
            first_index %= capacity;
            n--;
            Segment<K>*  temp = first->rightSibling;
            delete first;
            first = temp;
            first->leftSibling = nullptr;
            first_key = temp->keyStart;
        }

        queueMutex.unlock();
    }
    else
    {
        last_key = key;

        if (first->rightSibling && first->rightSibling->startingKeyPos == deletePos)
        {
            queueMutex.lock();

            first_index ++;
            first_index %= capacity;
            n--;
            Segment<K>*  temp = first->rightSibling;
            delete first;
            first = temp;
            first->leftSibling = nullptr;
            first_key = temp->keyStart;

            queueMutex.unlock();
        }
    }
    
    return;
}

/*
Clear PSFlirt
*/
template<class K>
void PSFlirt<K>::clear()
{
    if (n != 0)
    {
        Segment<K>* temp = first->rightSibling;
        delete first;
        while (temp != NULL)
        {
            Segment<K>* temp2 = temp->rightSibling;
            delete temp;
            temp = temp2;
        }
    }
    delete[] queue;
    queue = nullptr;

    queue = new pair<K,Segment<K>*>[capacity];
    first_index = 0;
    last_index = 0;
    n = 0;
    keyPos = 0;
}

/*
Print Content of Queue
*/
template<class K>
void PSFlirt<K>::printQueue()
{
    
    // Print first key
    for(int i = 0; i < n; i++)
    {
        cout << queue[i].first << "\t";
    }
    cout << endl;


    // //Print all keys
    // for(int i = 0; i < n; i++)
    // {
    //     for (auto &keys: queue[i].second->keys){
    //         if (!keys.deleted){
    //             cout << keys.key << " ";
    //         }
    //     }
    //     cout << " | ";
    // }
    // cout << endl;
}

/*
Get data structure size
*/
template<class K>
size_t PSFlirt<K>::get_model_size_in_bytes()
{
    size_t model_size = sizeof(Segment<K>*)*2 + sizeof(int)*5 + sizeof(K)*2 + sizeof(pair<K,Segment<K>*>*) + 
    sizeof(pair<K,Segment<K>*>)*n;

    for (int i=0; i<n;i++)
    {
        model_size += queue[i].second->get_model_size_in_bytes();
    }
    return model_size;
}


template<class K>
size_t PSFlirt<K>::get_total_size_in_bytes()
{
    size_t total_size = sizeof(Segment<K>*)*2 + sizeof(int)*5 + sizeof(K)*2 + sizeof(pair<K,Segment<K>*>*) + 
    sizeof(pair<K,Segment<K>*>)*n;

    for (int i=0; i<n;i++)
    {
        total_size += queue[i].second->get_total_size_in_bytes();
    }
    return total_size;
}

}
#endif