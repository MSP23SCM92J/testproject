#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <write.h>
#include <event.h>
#include <cstring>
#include <read.h>
#include <readQuery.h>

#define DATASET_NAME "story1"
#define H5FILE_NAME "data.h5"
#define LENGTH      10
#define RANK        1
#define FILE_NAME "input1.txt"
#define DEBUG       0
#define CHUNK_SIZE 1000

void writetofile(){

    std::vector<Event> storyChunk;
    __uint64_t time;
    std::string da;

    std::ifstream filePointer(FILE_NAME);
    filePointer.seekg(0, std::ios::beg);

    /*
    * Read data from file
    */
    Event e;
    while (filePointer >> time >> da) {
        e.timeStamp = time;
        strcpy(e.data, da.c_str());
        storyChunk.push_back(e);
    }
    filePointer.close();    
    for(int i=0;i<10;i++){
        std::cout<<storyChunk[i].timeStamp<<"\t"<<storyChunk[i].data<<"\n";
    }
    int status;
    
    //Write to dataset
    status = writeToDataset(storyChunk, FILE_NAME, DATASET_NAME, H5FILE_NAME, CHUNK_SIZE);
    if(!status)
    {
        std::cout<<"\nData written to file sucessfully!\n"<<std::endl;
    }
    else
    {
        std::cout<<"\nError occured while writing data to dataset\n"<<std::endl;
    }

    // Append to dataset
    // status = appendToDataset(storyChunk, FILE_NAME, DATASET_NAME, H5FILE_NAME);
    // if(!status)
    // {
    //     std::cout<<"\nData appended to file sucessfully!\n"<<std::endl;   
    // }
    // else
    // {
    //     std::cout<<"\nError occured while appending data to dataset\n"<<std::endl;
    // }
}

int main(int argc, char** argv) {


    // Write to dataset
    writetofile();    

    // Read Min and Max attribute from dataset
    std::pair<__uint64_t, __uint64_t> p = readDatasetRange(DATASET_NAME,H5FILE_NAME);
    std::cout<<"Min: "<<p.first<<" Max: "<<p.second<<std::endl;



    std::pair<int64_t, int64_t> range;
    range.first = 1093185;
    range.second = 1100000;
    std::vector<Event> e = readFromDataset(DATASET_NAME, range, H5FILE_NAME);
    if(e.size() != 0){
        std::cout << e.size() << std::endl;
        std::cout << e[0].timeStamp << std::endl;
        std::cout << e[e.size()-1].timeStamp << std::endl;
    }else{
        std::cout << "Empty result vector!\n";
    }

    // // Parallel reads
    testReadOperation();

    return 0;
}
