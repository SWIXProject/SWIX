#pragma once
#include "tuner_variables.hpp"
#include "timer_variables.hpp"

//Search Cost
extern uint64_t rootNoSearch;
extern uint64_t rootNoPredict;
extern uint64_t rootPredictCycle;
extern uint64_t rootExponentialSearchCycle;
extern double rootExponentialSearchCyclePerLength;

extern uint64_t segNoSearch;
extern uint64_t segNoPredict;
extern uint64_t segPredictCycle;
extern uint64_t segExponentialSearchCycle;
extern double segExponentialSearchCyclePerLength;

extern uint64_t bufferNoSearch;
extern uint64_t bufferLengthBinarySearch;
extern uint64_t bufferBinarySearchCycle;
extern double bufferBinarySearchCyclePerLength;

//Insertion Cost
extern uint64_t rootInsertCycle;
extern double rootInsertCyclePerLength;

extern uint64_t segNoInsert;
extern uint64_t segInsertCycle;

extern uint64_t rootNoInsert;
extern uint64_t rootSizeInsert;

//Retrain Cost
extern uint64_t rootNoRetrain;
extern uint64_t rootLengthRetrain;
extern uint64_t rootRetrainCycle;
extern double rootRetrainCyclePerLength;

extern uint64_t segMergeCycle;
extern double segMergeCyclePerLength;
extern uint64_t segRetrainCycle;
extern double segRetrainCyclePerLength;