#ifndef __FLIRT_HELPER_HPP__
#define __FLIRT_HELPER_HPP__

#pragma once
#include<iostream>
#include<algorithm>
#include<iterator>
#include<vector>
#include<math.h>
#include<time.h>

/*
Functions used in FLIRT and FLIRT Multithread.
*/

using namespace std;

namespace flirt{

/*
data struct
*/
template <class T>
struct Key 
{
	T key;
	bool deleted;
};

/*
Function Headers
*/
template<class T>
int lower_bound_array(const T* const ptr, T x, int n);

template<class T>
void merge_arrays(const T* const ptr1, int n1, const T* const ptr2, int n2, T* const output);

template<class T>
void merge_array_vector_to_vector(const T* const ptr1, int n1, const vector<T>& v2, vector<T>& output);

template<class T>
void merge_array_vector_to_vector_key(const T* const ptr1, int n1, const vector<Key<T>>& v2, vector<T>& output);

template<class T>
void merge_arrays_to_vector(const T* const ptr1, int n1, const T* const ptr2, int n2, vector<T>& output);

template<class T>
void merge_vectors(const vector<T>& v1, const vector<T>& v2, vector<T>& output);

template<class T>
bool binary_search_array(const T* const ptr, int n, int posMin, int posMax, T key);

template<class T>
int binary_search_array_return_index(const T* const ptr, int n, int posMin, int posMax, T key);

template<class T>
bool binary_search_vector(const vector<T>& keys, int posMin, int posMax, T & key);

template<class T>
bool binary_search_vector(const vector<T>& keys, int posMin, int posMax, T & key, int & actualPos, int & deletedPos);

template<class T>
bool binary_search_vector_key(const vector<Key<T>>& keys, int posMin, int posMax, T & key);

template<class T>
bool binary_search_vector_key(const vector<Key<T>>& keys, int posMin, int posMax, T & key, int & actualPos, int & deletedPos);

template<class T>
int binary_search_vector_return_index(const vector<T>& keys, int posMin, int posMax, T key);

template<class T>
int binary_search_vector_key_return_index(const vector<Key<T>>& keys, int posMin, int posMax, T key);

template <class T>
vector<T> slice_vector(vector<T> const& v, int indexStart, int indexEnd);

template <class T>
vector<Key<T>> slice_vector_key(vector<T> const& v, int indexStart, int indexEnd);

/*
Function Implementations
*/
template<class T>
int lower_bound_array(const T* const ptr, T x, int n) 
{
	int index = lower_bound(ptr, ptr + n - 1, x) - ptr;
	if (ptr[index] == x) {
		return index;
	}
	else {
		if (index == n - 1) {return index;}
		else { return index - 1;}
	}
}

template<class T>
void merge_arrays(const T* const ptr1, int n1,
	const T* const ptr2, int n2,
	T* const output) 
{

	int i = 0, j = 0, k = 0;

	while (i < n1 && j < n2) {
		if (ptr1[i] < ptr2[j]) { output[k++] = ptr1[i++]; }
		else { output[k++] = ptr2[j++]; }
	}
	while (i < n1) { output[k++] = ptr1[i++]; }
	while (j < n2) { output[k++] = ptr2[j++]; }
}

template<class T>
void merge_array_vector_to_vector(const T* const ptr1, int n1,
	const vector<T>& v2,
	vector<T>& output) 
{
	int i = 0, k = 0;
	size_t j = 0;

	while (i < n1 && j < v2.size()) {
		if (ptr1[i] < v2[j]) { output[k++] = ptr1[i++];; }
		else { output[k++] = v2[j++]; }
	}
	while (i < n1) { output[k++] = ptr1[i++]; }
	while (j < v2.size()) { output[k++] = v2[j++]; }
}

template<class T>
void merge_array_vector_to_vector_key(const T* const ptr1, int n1,
	const vector<Key<T>>& v2,
	vector<T>& output) 
{
	int i = 0, k = 0;
	size_t j = 0;

	while (i < n1 && j < v2.size()) {
		if (ptr1[i] < v2[j].key) { output[k++] = ptr1[i++]; }
		else { if (v2[j].deleted == false) { output[k++] = v2[j++].key; } else { j++; } }
	}
	while (i < n1) {output[k++] = ptr1[i++];}
	while (j < v2.size()) { if (v2[j].deleted == false) { output[k++] = v2[j++].key; } else { j++;} }
}

template<class T>
void merge_arrays_to_vector(const T* const ptr1, int n1,
	const T* const ptr2, int n2,
	vector<T>& output) 
{
	int i = 0, j = 0; int k = 0;

	while (i < n1 && j < n2) {
		if (ptr1[i] < ptr2[j]) { output[k++] = ptr1[i++];; }
		else { output[k++] = ptr2[j++]; }
	}
	while (i < n1) { output[k++] = ptr1[i++]; }
	while (j < n2) { output[k++] = ptr2[j++]; }
}

template<class T>
void merge_vectors(const vector<T>& v1, const vector<T>& v2, vector<T>& output) 
{
	int i = 0, j = 0; int k = 0;

	while (i < v1.size() && j < v2.size()) {
		if (v1[i] < v2[j]) { output[k++] = v1[i++];; }
		else { output[k++] = v2[j++]; }
	}
	while (i < v1.size()) { output[k++] = v1[i++]; }
	while (j < v2.size()) { output[k++] = v2[j++]; }
}

template<class T>
bool binary_search_array(const T* const ptr, int n, int posMin, int posMax, T key) 
{
	if (posMin < 0) { posMin = 0; }
	if (posMax > n - 1) { posMax = n - 1; }

	while (posMin <= posMax) {
		int mid = posMin + (posMax - posMin) / 2;
		if (ptr[mid] == key) { return true; }
		if (ptr[mid] < key) { posMin = mid + 1; }
		else { posMax = mid - 1; }
	}
	return false;
}

template<class T>
int binary_search_array_return_index(const T* const ptr, int n, int posMin, int posMax, T key) 
{
	if (posMin < 0) { posMin = 0; }
	if (posMax > n - 1) { posMax = n - 1; }

	while (posMin <= posMax) {
		int mid = posMin + (posMax - posMin) / 2;
		if (ptr[mid] == key) { return mid; }
		if (ptr[mid] < key) { posMin = mid + 1; }
		else { posMax = mid - 1; }
	}
	return -1;
}

template<class T>
bool binary_search_vector(const vector<T>& keys, int posMin, int posMax, T & key) 
{
	if (posMin < 0) { posMin = 0; }
	if (posMax > keys.size() - 1) { posMax = keys.size() - 1; }

	while (posMin <= posMax) {
		int mid = posMin + (posMax - posMin) / 2;
		if (keys[mid] == key) { return true; }
		if (keys[mid] < key) { posMin = mid + 1; }
		else { posMax = mid - 1; }
	}
	return false;
}

template<class T>
bool binary_search_vector(const vector<T>& keys, int posMin, int posMax, T & key, int & actualPos, int & deletedPos) 
{
	if (posMin < 0) { posMin = 0; }
	if (posMax > keys.size() - 1) { posMax = keys.size() - 1; }

	while (posMin <= posMax) {
		int mid = posMin + (posMax - posMin) / 2;
		if (keys[mid] == key) { return (actualPos + mid > deletedPos); }
		if (keys[mid] < key) { posMin = mid + 1; }
		else { posMax = mid - 1; }
	}
	return false;
}

template<class T>
bool binary_search_vector_key(const vector<Key<T>>& keys, int posMin, int posMax, T & key) 
{
	if (posMin < 0) { posMin = 0; }
	if (posMax > keys.size() - 1) { posMax = keys.size() - 1; }

	while (posMin <= posMax) {
		int mid = posMin + (posMax - posMin) / 2;
		if (keys[mid].key == key) { return !keys[mid].deleted; }
		if (keys[mid].key < key) { posMin = mid + 1; }
		else { posMax = mid - 1; }
	}
	return false;
}


template<class T>
bool binary_search_vector_key(const vector<Key<T>>& keys, int posMin, int posMax, T & key, int & actualPos, int & deletedPos) 
{
	if (posMin < 0) { posMin = 0; }
	if (posMax > keys.size() - 1) { posMax = keys.size() - 1; }

	while (posMin <= posMax) {
		int mid = posMin + (posMax - posMin) / 2;
		if (keys[mid].key == key) 
		{ 		
			return (actualPos + mid > deletedPos);
		}
		if (keys[mid].key < key) { posMin = mid + 1; }
		else { posMax = mid - 1; }
	}
	return false;
}


template<class T>
int binary_search_vector_return_index(const vector<T>& keys, int posMin, int posMax, T key) 
{
	if (posMin < 0) { posMin = 0; }
	if (posMax > keys.size() - 1) { posMax = keys.size() - 1; }

	while (posMin <= posMax) {
		int mid = posMin + (posMax - posMin) / 2;
		if (keys[mid] == key) { return mid; }
		if (keys[mid] < key) { posMin = mid + 1; }
		else { posMax = mid - 1; }
	}
	return -1;
}

template<class T>
int binary_search_vector_key_return_index(const vector<Key<T>>& keys, int posMin, int posMax, T key) 
{
	if (posMin < 0) { posMin = 0; }
	if (posMax > keys.size() - 1) { posMax = keys.size() - 1; }

	while (posMin <= posMax) {
		int mid = posMin + (posMax - posMin) / 2;
		if (keys[mid].key == key) { return mid; }
		if (keys[mid].key < key) { posMin = mid + 1; }
		else { posMax = mid - 1; }
	}
	return -1;
}

template <class T>
vector<T> slice_vector(vector<T> const& v, int indexStart, int indexEnd) 
{
	auto first = v.begin() + indexStart;
	auto last = v.begin() + indexEnd + 1;
	vector<T> vector(first, last);
	return vector;
}

template <class T>
vector<Key<T>> slice_vector_key(vector<T> const& v, int indexStart, int indexEnd) 
{
	vector<Key<T>> output(indexEnd - indexStart + 1);
	int counter = 0;
	for (auto first = v.begin() + indexStart, last = v.begin() + indexEnd + 1; first != last; ++first) {
		output[counter++] = { *first,false };
	}
	return output;
}

}
#endif