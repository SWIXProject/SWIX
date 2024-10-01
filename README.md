# SWIX: A Memory-efficient Sliding Window Learned Index

# Introduction
SWIX is a memory efficient learned index for sliding windows of streams. Details of SWIX can be found our [SIGMOD 2024 paper](https://dl.acm.org/doi/pdf/10.1145/3639296). We also included the code of [FLIRT](lib/flirt/), an incremental learned index. Details can be found in our [EDBT 2023 paper](https://openproceedings.org/2023/conf/edbt/paper-219.pdf). Comparative analysis containing all datasets are in [experiments/](experiments/). Latest figures will be added soon. Note the full disscussion of the results will be included in the extended version of the paper (Coming soon!)

### Table of Contents
**[Getting Started](##getting-started)**<br>
**[License](##license)**<br>

## Getting Started

SWIX is header-only library. It does not need any installation. You can use the library after cloning the repo with any tool of your choice. For example:

```bash
git clone https://github.com/SWIXProject/SWIX.git
cd SWIX
```

To use the SWIX library on it's own, you just need to copy [src/](src/) directory to your project directory or to the project's include path.

For running SWIX, an example can be found in [main.cpp](main.cpp). Specifically, it shows how to bulkload, lookup, range search, insert using SWIX.

The dataset here is from the [SOSD benchmark](https://github.com/learnedsystems/SOSD/blob/master/scripts/download.sh). We stored the data at `/data/Documents/data/', feel free to change it to the directory where you stored the data. 

Alternatively, you can run with any randomly generated data which is the default option.

```cpp
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <iterator>
#include <random>
#include <cmath>
#include <fstream>

#include "src/Swix.hpp"
#include "src/SwixTuner.hpp"
#include "utils/load.hpp"

using namespace std;

// #define RUN_SOSD

//Place Holders
#ifdef RUN_SOSD
string data_dir =  "/data/Documents/data/";
string data_file = "f_books";
#endif

int matchRate = 1000;
int seed = 10;

void load_data(vector<tuple<uint64_t, uint64_t, uint64_t>> & data)
{
    string input_file = data_dir+data_file;
    add_timestamp(input_file, data, matchRate ,seed);
}

int main()
{
    //load data
    vector<tuple<uint64_t, uint64_t, uint64_t>> data;
    data.reserve(TEST_LEN);
    load_data(data);

    //Initialize Data
    vector<pair<uint64_t, uint64_t>> data_initial;
    data_initial.reserve(TIME_WINDOW);

    //Using SOSD data
    #ifdef RUN_SOSD
    for (auto it = data.begin(); it != data.begin()+TIME_WINDOW; it++)
    {
        data_initial.push_back(make_pair(get<0>(*it),get<1>(*it)));
    }
    #endif

    // Random data
    std::mt19937 gen(seed);
    std::uniform_int_distribution<uint64_t> distr(1, MAX_TIMESTAMP);
    for (int i = 0; i < TIME_WINDOW; i++)
    {
        data_initial.push_back(make_pair(i, distr(gen)));
    }
    sort(data_initial.begin(),data_initial.end(),sort_based_on_second);

    //Bulkload
    swix::SWmeta<uint64_t,uint64_t> Swix(data_initial);

    //Lookup
    pair<uint64_t,uint64_t> searchPair = make_pair(get<0>(data[int(TIME_WINDOW/2)]), get<0>(data[int(TIME_WINDOW/2)]));
    uint64_t resultCount = 0;
    Swix.lookup(searchPair,resultCount);

    //Range Search
    tuple<uint64_t,uint64_t,uint64_t> searchTuple = data[int(TIME_WINDOW/2)];
    vector<pair<uint64_t, uint64_t>> searchResult;
    Swix.range_search(searchTuple,searchResult);

    //Insert
    pair<uint64_t,uint64_t> insertPair = make_pair(get<0>(data[int(TIME_WINDOW+10)]), get<0>(data[int(TIME_WINDOW+10)]));
    Swix.insert(insertPair);

    return 0;
}
```

You can use the makefile to run the test. 
``` bash
make
./z_run_test.out
```

Alternatively, to run this locally:

```bash
g++ main.cpp -std=c++17 -fopenmp -march=native -O3 -w -o z_run_test.out
./z_run_test.out
```
To run Parallel SWIX, run the class in [run_pswix.hpp](benchmark/run_pswix.hpp) or [run_pswix_v2.hpp](benchmark/run_pswix_v2.hpp). V2 is the one evualated in the paper and limits cross parition modification. 

Note: to run Parallel SWIX or the benchmarks, remember to manually change the `DATA_DIR` in [parameters_p.hpp](parameters_p.hpp) and [parameters.hpp](parameters.hpp).

To run this locally (note that [Intel MKL](https://www.intel.com/content/www/us/en/developer/tools/oneapi/onemkl.html) is required to run the parallel indexes):

``` bash
g++ benchmark/run_pswix_v2.cpp -std=c++17 -I/opt/intel/oneapi/mkl/2023.1.0/include -fopenmp -g -msse -march=core-avx2 -O3 -pthread -w -o z_run_test.out
```

## License

This project is licensed under the terms of the MIT License.

If you want to use the library, please cite the paper:

> Liang Liang, Guang Yang, Ali Hadian, Luis Alberto Croquevielle, and Thomas Heinis. 2024. SWIX: A Memory-efficient Sliding Window Learned Index. Proc. ACM Manag. Data 2, 1, Article 41 (February 2024), 26 pages. https://doi.org/10.1145/3639296

```tex
@article{liang2024swix,
  title={SWIX: A Memory-efficient Sliding Window Learned Index},
  author={Liang, Liang and Yang, Guang and Hadian, Ali and Croquevielle, Luis Alberto and Heinis, Thomas},
  journal={Proceedings of the ACM on Management of Data},
  volume={2},
  number={1},
  pages={1--26},
  year={2024},
  publisher={ACM New York, NY, USA}
}
```


