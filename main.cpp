#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <mpi.h>
#include <H5Cpp.h>
#include <write.h>
#include <event.h>
#include <read.h>
#include <readQuery.h>

#define DATASET_NAME "story1"
#define H5FILE_NAME "data.h5"
#define LENGTH      10
#define RANK        1
#define FILE_NAME "input1.txt"


int main(int argc, char** argv) {

    // Write to dataset
    int status = writeToDataset(FILE_NAME, DATASET_NAME, H5FILE_NAME);

    if(!status)
    {
        std::cout<<"\nData written to file sucessfully!\n"<<std::endl;   
    }
    else
    {
        std::cout<<"\nError occured while writing data to dataset\n"<<std::endl;
    }


    // Single read request
    std::pair<int64_t, int64_t> range;
    range.first = 1094691;
    range.second = 1099375;
    std::vector<Event> e = readFromDataset(DATASET_NAME, range, H5FILE_NAME);
    std::cout<<e[0].timeStamp<<std::endl;
    std::cout<<e[e.size()-1].timeStamp<<std::endl;

    // Multiple parallel reads
    // multiple_reads();

    return 0;
}