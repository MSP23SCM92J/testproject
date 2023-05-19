#include <iostream>
#include <event.h>
#include <datasetreader.h>
#include <datasetminmax.h>
#ifndef READ_H
#define READ_H

class storyReader
{
private:
    /* data */
public:
    storyReader(/* args */);
    ~storyReader();

    // Read data from H5 dataset
    DatasetReader readFromDataset(std::pair<uint64_t, uint64_t> range, const char* STORY, const char* CHRONICLE);

    // Read dataset data range i.e Min and Max
    DatasetMinMax readDatasetRange(const char* STORY, const char* CHRONICLE);

};

#endif