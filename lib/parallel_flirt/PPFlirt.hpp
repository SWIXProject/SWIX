#ifndef __FLIRT_CONCURRENT_HPP__
#define __FLIRT_CONCURRENT_HPP__

#pragma once
#ifdef __FLIRT_HELPER_HPP__
#inclde "../flirt/flirt_helper.hpp"
#else
#include "flirt_helper.hpp"
#endif

using namespace std;

#ifndef INITIAL_ERROR
#define GLOBAL_ERROR 256
#else
#define GLOBAL_ERROR INITIAL_ERROR
#endif

namespace flirt {

template<class K> class PPFlirt;

/*
***************** LP Segments *****************
*/
template<class K>
class Segment
{
private:	
    int n;    //current number of keys
    int startingKeyPos; // starting key position in entire queue;

	K keyStart; //starting key
    
	double slope;
	double slopeLow;  //Low Slope of Segment
	double slopeHigh; //High Slope of Segment

	vector<K> keys;   //Vector of keys
	Segment<K>* leftSibling = nullptr; //Pointer to the left sibling (leaf only)
	Segment<K>* rightSibling = nullptr; //Pointer to the right sibling (leaf only)

public:
    //Constructor
	Segment(K keyStart, int startingKeyPos, double slopeLow = 0, double slopeHigh = numeric_limits<double>::max());

    //Search
	bool lookup(K & key);
    bool lookup(K & key, int & deletedPos);
    void range_search(K key, K lowerBound, K upperBound, int deletePos, vector<K>& results);

private:
    //Search Helpers
    void range_scan(int actualPos, K key, K lowerBound, K upperBound, vector<K>& results);
    void range_scan(int actualPos, K key, K lowerBound, K upperBound, int deletePos, vector<K>& results);

public:
    //Enqueue
	Segment<K>* push_back(K key, int startingKeyPos);

    //Getters
    size_t get_model_size_in_bytes();
    size_t get_data_size_in_bytes();
    size_t get_total_size_in_bytes();

    friend class PPFlirt<K>;
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
    this->startingKeyPos = startingKeyPos;
}

/*
Search
*/
template<class K>
bool Segment<K>::lookup(K & key) 
{
	if (n == 1) {
		return (keys[0] == key);
	}

	int pos = (key - keyStart) * slope;
    return binary_search_vector(keys, pos - GLOBAL_ERROR, pos + GLOBAL_ERROR, key );
}

template<class K>
bool Segment<K>::lookup(K & key, int & deletedPos) 
{
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
    return binary_search_vector(keys, pos - GLOBAL_ERROR, pos + GLOBAL_ERROR, key, startingKeyPos, deletedPos);
}

template<class K>
void Segment<K>::range_search(K key, K lowerBound, K upperBound, int deletePos, vector<K> & results)
{
    int predictPos = (lowerBound - keyStart) * slope;
    
    int actualPos = binary_search_vector_return_index(keys, predictPos-GLOBAL_ERROR,predictPos+GLOBAL_ERROR, lowerBound);
    actualPos = (actualPos == -1)? n : actualPos;

    (deletePos == -1) ? range_scan(actualPos, key, lowerBound, upperBound, results):
            range_scan(actualPos, key, lowerBound, upperBound, deletePos, results);
}

template<class K>
void Segment<K>::range_scan(int actualPos, K key, K lowerBound, K upperBound, vector<K> & results)
{
    while (actualPos < n && keys[actualPos] <= upperBound)
    {
        if (keys[actualPos] >= lowerBound)
        {
            results.push_back(keys[actualPos]);
        }
        actualPos++;
    }

    if (rightSibling && rightSibling->keyStart <= upperBound)
    {
        rightSibling->range_scan(0,key,lowerBound,upperBound,results);
    }
}

template<class K>
void Segment<K>::range_scan(int actualPos, K key, K lowerBound, K upperBound, int deletePos, vector<K> & results)
{
    if (startingKeyPos + actualPos > deletePos)
    {
        while (actualPos < n && keys[actualPos] <= upperBound)
        {
            if (keys[actualPos] >= lowerBound)
            {
                results.push_back(keys[actualPos]);
            }
            actualPos++;
        } 
    }
    else if (startingKeyPos + n-1 > deletePos)
    {
        while (actualPos < n && keys[actualPos] <= upperBound)
        {
            if (startingKeyPos + actualPos > deletePos && keys[actualPos] >= lowerBound)
            {
                results.push_back(keys[actualPos]);
            }
            actualPos++;
        }
    }

    if (rightSibling && rightSibling->keyStart <= upperBound)
    {
        rightSibling->range_scan(0,key,lowerBound,upperBound,results);
    }
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

		slopeHigh = min(slopeHigh, slopeHighTemp);
		slopeLow = max(slopeLow, slopeLowTemp);
		slope = (slopeHigh + slopeLow) / 2;
		keys.push_back(key);
		n++;
		return nullptr;
	}
	//Else create new segment
	else {
		Segment<K>* segPtr = new Segment<K>(key, startingKeyPos);
		rightSibling = segPtr;
		segPtr->leftSibling = this;
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
size_t Segment<K>::get_data_size_in_bytes()
{
    return sizeof(K)*n;
}

template<class K>
size_t Segment<K>::get_total_size_in_bytes()
{
    return sizeof(int)*2 + sizeof(double)*3 + sizeof(K) + sizeof(Segment<K>*)*2 + sizeof(vector<K>)
    + sizeof(K)*n;
}



/*
***************** local FLIRT for each partition *****************
*/
template<class K>
class PPFlirt 
{
private:
    int n; // current size of the queue
    int capacity; //capacity
    int first_index;  //front index
    int last_index;  //last index
    int keyPos; //position of key in queue

    K first_key; //First key in PPFlirt
    K last_key; //Last key in PPFlirt

    Segment<K>* first = nullptr; //first segment in queue
    Segment<K>* last = nullptr; //last segment in queue

    pair<K,Segment<K>*>* queue; //queue which is the SummaryList

public:
    //Constructor & Destructor
    PPFlirt(int size);  
    ~PPFlirt();

    //Search
    bool lookup(K target);
    bool lookup(K target, int deletePos);

    void range_search(K target, K lowerBound, K upperBound, vector<K> & results, int deletePos = -1);
    
private:
    //Search Helpers
    bool linear_search(K target, int left, int right);
    bool binary_search(K target, int left, int right);

    bool linear_search(K target, int left, int right, int & deletePos);
    bool binary_search(K target, int left, int right, int & deletePos);

    void linear_range_search(K target, K lowerBound, K upperBound, int left, int right, int deletePos, vector<K> & results);
    void binary_range_search(K target, K lowerBound, K upperBound, int left, int right, int deletePos, vector<K> & results);

public:
    //Enqueue
    void enqueue(K key);
    
    //Utils
    void clear();
    void printQueue();
    size_t get_model_size_in_bytes();
    size_t get_data_size_in_bytes();
    size_t get_total_size_in_bytes();
    size_t get_size();
};

/*
Constructor & Destructor
*/
template<class K>
PPFlirt<K>::PPFlirt(int size)
{
    queue = new pair<K,Segment<K>*>[size];
    capacity = size;
    first_index = 0;
    last_index = 0;
    n = 0;
    keyPos = 0;
}

template<class K>
PPFlirt<K>::~PPFlirt()
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
bool PPFlirt<K>::lookup(K target)
{
    if (n == 0)
    {
        // cout << "Empty Queue" << endl;
        return false;
    }

    if (n*sizeof(K) < 256)
    {
        return linear_search(target,first_index,last_index);
    }
    else
    {
        return binary_search(target,first_index,last_index);
    }
}


template<class K>
bool PPFlirt<K>::lookup(K target, int deletePos)
{
    if (n == 0)
    {
        // cout << "Empty Queue" << endl;
        return false;
    }

    if (n*sizeof(K) < 256)
    {
        return linear_search(target,first_index,last_index, deletePos);
    }
    else
    {
        return binary_search(target,first_index,last_index, deletePos);
    }
}

template<class K>
void PPFlirt<K>::range_search(K target, K lowerBound, K upperBound, vector<K> & results, int deletePos)
{
    if (n == 0)
    {
        // cout << "Empty Queue" << endl;
        return;
    }

    K startingKey = max(first_key, lowerBound);
    K endingKey = min(last_key, upperBound);

    if (startingKey < endingKey) //Within search bound
    {
        if (n*sizeof(K) < 256) //Less than 256 bytes: linear search
        {
            return linear_range_search(target,lowerBound,upperBound,first_index,last_index,deletePos,results);
        }

        if (last_index < first_index) //Else Binary
        {
            if(target > queue[capacity-1].first) // not in second half arr
            {
                return binary_range_search(target, lowerBound, upperBound, 0, last_index, deletePos, results);
            }
            else // maybe in second half arr
            {
                return binary_range_search(target, lowerBound, upperBound, first_index, capacity-1, deletePos, results);
            }
        }
        else
        {
            return binary_range_search(target, lowerBound, upperBound, first_index, last_index, deletePos, results);
        }

    }
    else if (startingKey == endingKey) //Either in first segment or last segment
    {
        if (startingKey == first_key)
        {
            return queue[first_index].second->range_search(target, lowerBound, upperBound, deletePos, results);
        }
        else
        {
            return queue[last_index].second->range_search(target, lowerBound, upperBound, deletePos, results);
        }
    }
}

/*
Linear Search Method used in Search Operation
For finding the segment in SummaryList
*/
template<class K>
bool PPFlirt<K>::linear_search(K target, int left, int right)
{
    int index = left;
    while (index < right && queue[index+1].first <= target) {index++;}
    return queue[index].second->lookup(target);
}

template<class K>
bool PPFlirt<K>::linear_search(K target, int left, int right, int & deletePos)
{
    int index = left;
    while (index < right && queue[index+1].first <= target) {index++;}
    return queue[index].second->lookup(target, deletePos);
}

template<class K>
void PPFlirt<K>::linear_range_search(K target, K lowerBound, K upperBound, int left, int right, int deletePos, vector<K> & results)
{
   for(int i = left;  i < right; i++)
   {
       if (queue[i].first <= lowerBound && lowerBound < queue[i+1].first)
       {
           queue[i].second->range_search(target,lowerBound,upperBound,deletePos,results);
           return;
       }
   }
    if (queue[right].first <= lowerBound)
    {
        queue[right].second->range_search(target,lowerBound,upperBound,deletePos,results);
        return;
    }
   return;
}

/*
Binary Search Method used in Search Operation
For finding the segment in SummaryList
*/
template<class K>
bool PPFlirt<K>::binary_search(K target, int left, int right)
{
    
    //edge case (left  boundary)
    if (target < queue[left+1].first)
    {
        if (queue[left].first <= target)
        {
            return queue[left].second->lookup(target);
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
            return queue[right-1].second->lookup(target);
        }
        else
        {
            return queue[right].second->lookup(target);
        }
    }

    while(left <= right)
    {
        int mid = (left + right) / 2;
        if (queue[mid].first == target)
        {
            return queue[mid].second->lookup(target);
        }
        if (queue[mid].first < target)
        {
            if (queue[mid-1].first >= target)
            {
                return queue[mid-1].second->lookup(target);  
            }
            left = mid + 1;
        }
        else
        {
            if (queue[mid+1].first < target)
            {
                return queue[mid].second->lookup(target);  
            }

            right = mid - 1;
        }
    }

    return false;
}

template<class K>
bool PPFlirt<K>::binary_search(K target, int left, int right, int & deletePos)
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

template<class K>
void PPFlirt<K>::binary_range_search(K target, K lowerBound, K upperBound, int left, int right, int deletePos, vector<K> & results)
{
    //edge case (left  boundary)
    if (lowerBound < queue[left+1].first)
    {
        if (queue[left].first <= lowerBound)
        {
            return queue[left].second->range_search(target,lowerBound,upperBound,deletePos,results);
        }
        else
        {
            return;
        }
    }

    //edge case (right boundary)
    if (lowerBound >= queue[right-1].first)
    {
        if (lowerBound < queue[right].first)
        {
            return queue[right-1].second->range_search(target,lowerBound,upperBound,deletePos,results);
        }
        else
        {
            return queue[right].second->range_search(target,lowerBound,upperBound,deletePos,results);
        }
    }

    while(left <= right)
    {
        int mid = (left + right) / 2;
        if (queue[mid].first <= lowerBound && queue[mid+1].first > lowerBound)
        {
            return queue[mid].second->range_search(target,lowerBound,upperBound,deletePos,results);
        }
        if (queue[mid].first < lowerBound)
        {
            left = mid + 1;
        }
        else
        {
            right = mid - 1;
        }
    }
    return;
}

/*
Enqueue 
*/
template<class K>
void PPFlirt<K>::enqueue(K key)
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
Print Content of Queue
*/
template<class K>
void PPFlirt<K>::printQueue()
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
Clear PPFlirt
*/
template<class K>
void PPFlirt<K>::clear()
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
Get data structure size
*/
template<class K>
size_t PPFlirt<K>::get_model_size_in_bytes()
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
size_t PPFlirt<K>::get_data_size_in_bytes()
{
    size_t data_size = 0;

    for (int i=0; i<n;i++)
    {
        data_size += queue[i].second->get_data_size_in_bytes();
    }
    return data_size;
}

template<class K>
size_t PPFlirt<K>::get_total_size_in_bytes()
{
    size_t total_size = sizeof(Segment<K>*)*2 + sizeof(int)*5 + sizeof(K)*2 + sizeof(pair<K,Segment<K>*>*) + 
    sizeof(pair<K,Segment<K>*>)*n;

    for (int i=0; i<n;i++)
    {
        total_size += queue[i].second->get_total_size_in_bytes();
    }
    return total_size;
}

template<class K>
size_t PPFlirt<K>::get_size()
{
    int size = 0;
    for(int i = 0; i < n; i++)
    {
        size += queue[i].second->n;
    }
    return size;
}

}
#endif
