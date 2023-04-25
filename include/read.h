#include <iostream>
#include <event.h>
#ifndef READ_H
#define READ_H

// Read data from H5 dataset
std::vector<Event> readFromDataset(const char* DATSET_NAME, std::pair<int64_t, int64_t> range, const char* H5FILE_NAME);

// Read dataset data range i.e Min and Max
std::pair<__uint64_t, __uint64_t> readDatasetRange(const char* DATASET_NAME, const char* H5FILE_NAME);

#endif