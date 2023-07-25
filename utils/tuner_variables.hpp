#pragma once

//Current Tune Stage
extern uint64_t tuneStage;

//Search Cost
extern uint64_t rootNoExponentialSearch;
extern uint64_t rootLengthExponentialSearch;

extern uint64_t segNoExponentialSearch;
extern uint64_t segLengthExponentialSearch;

//Scan Cost
extern uint64_t segNoScan;
extern uint64_t segScanNoSeg;

//Retrain Cost
extern uint64_t segNoRetrain;
extern uint64_t segLengthMerge;
extern uint64_t segLengthRetrain;

extern uint64_t segNoExponentialSearchAll;
extern uint64_t segLengthExponentialSearchAll;