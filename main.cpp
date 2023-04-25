#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <write.h>
#include <event.h>
#include <read.h>
#include <readQuery.h>

#define DATASET_NAME "story1"
#define H5FILE_NAME "data.h5"
#define LENGTH      10
#define RANK        1
#define FILE_NAME "input2.txt"
#define DEBUG       0
#define CHUNK_SIZE 10000

int main(int argc, char** argv) {

    int status;

    // Write to dataset
    status = writeToDataset(FILE_NAME, DATASET_NAME, H5FILE_NAME, CHUNK_SIZE);
    if(!status)
    {
        std::cout<<"\nData written to file sucessfully!\n"<<std::endl;
    }
    else
    {
        std::cout<<"\nError occured while writing data to dataset\n"<<std::endl;
    }

    // Append to dataset
    /*
    status = appendToDataset(FILE_NAME, DATASET_NAME, H5FILE_NAME);
    if(!status)
    {
        std::cout<<"\nData appended to file sucessfully!\n"<<std::endl;   
    }
    else
    {
        std::cout<<"\nError occured while appending data to dataset\n"<<std::endl;
    }
    */

    // Read Min and Max attribute from dataset
    std::pair<__uint64_t, __uint64_t> p = readDatasetRange(DATASET_NAME,H5FILE_NAME);
    std::cout<<"Min: "<<p.first<<" Max: "<<p.second<<std::endl;


    /*
    std::pair<int64_t, int64_t> range;
    range.first = 8959706595337623745;
    range.second = 9107838454734240328;
    std::vector<Event> e = readFromDataset(DATASET_NAME, range, H5FILE_NAME);
    if(e.size() != 0){
        std::cout<<e.size()<<std::endl;
        std::cout<<e[0].timeStamp<<std::endl;
        std::cout<<e[e.size()-1].timeStamp<<std::endl;
        // for(int i=0; i<e.size();i++) {
        //     std::cout<<e[i].timeStamp<<std::endl;
        // }
    }else{
        std::cout<<"Empty result vector!\n";
    }
    */

    // // Parallel reads
    testReadOperation();

    return 0;
}
