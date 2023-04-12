#include <iostream>
#include <string>
#include <vector>
#include "H5Cpp.h"
#include <read.h>
#include <event.h>
#include <chunkattr.h>

#define CHUNK_METADATA "ChunkMetadata"
#define CHUNK_SIZE 1000
#define DIMENSION 1

// Read data from H5 dataset returns empty vector if range not within limits else returns vector filled with Events data
std::vector<Event> readFromDataset(const char* DATASET_NAME, std::pair<int64_t, int64_t> range, const char* H5FILE_NAME){

    std::vector<Event> r1;
    hid_t   file, dataset, space, dataspace, s2_tid;
    hsize_t dims_out[DIMENSION];
    herr_t  status;

    /*
     * read data from the dataset
     */
    file = H5Fopen(H5FILE_NAME, H5F_ACC_RDONLY, H5P_DEFAULT);
    dataset = H5Dopen2(file, DATASET_NAME, H5P_DEFAULT);
    hid_t datatype = H5Dget_type(dataset);
    dataspace  = H5Dget_space(dataset);
    status = H5Sget_simple_extent_dims(dataspace, dims_out, NULL);
    // Open the attribute
    hid_t attribute_id = H5Aopen(dataset, CHUNK_METADATA, H5P_DEFAULT);

    // Get the attribute type and dataspace
    hid_t attribute_type = H5Aget_type(attribute_id);
    hid_t dataspace_id = H5Aget_space(attribute_id);

    // Get the number of dimensions of the dataspace
    hsize_t ndims = H5Sget_simple_extent_npoints(dataspace_id);
    // std::cout<<ndims<<std::endl;
    ChunkAttr* attribute_data = (ChunkAttr *) malloc(ndims * sizeof(ChunkAttr));

    // Read the attribute data into the buffer
    H5Aread(attribute_id, attribute_type, attribute_data);

    // Print the attribute data
    // for (int i = 0; i < ndims; i++) {
    //     std::cout<<attribute_data[i].start<<"\t"<<attribute_data[i].end<<std::endl;
    // }

    std::pair<int64_t, int64_t> chunksToRetrieve = {-1 , -1};
    hsize_t i = 0;
    // Find the starting chunk index
    for(i=0 ; i < ndims ; i++){
        if(attribute_data[i].start <= range.first && attribute_data[i].end >= range.first)
        {
            chunksToRetrieve.first = i;
            break;
        }
    }
    
    // Find the ending chunk index
    for( ; i < ndims ; i++){
        if(attribute_data[i].start <= range.second && attribute_data[i].end >= range.second)
        {
            chunksToRetrieve.second = i;
            break;
        }
    }
    
    // Check for the range of requested query of chunks are not found
    if(chunksToRetrieve.second == -1 || chunksToRetrieve.first == -1){
        H5Dclose(dataset);
        H5Fclose(file);
        H5Aclose(attribute_id);
        H5Tclose(attribute_type);
        return r1;
    }
    // std::cout<<chunksToRetrieve.first<<"\t"<<chunksToRetrieve.second<<std::endl;

    // Event* eventBuffer = (Event*) malloc((chunksToRetrieve.second - chunksToRetrieve.first + 1) * sizeof(Event));
    Event* eventresult = (Event*) malloc(CHUNK_SIZE * sizeof(Event));

    s2_tid = H5Tcreate(H5T_COMPOUND, sizeof(Event));
    H5Tinsert(s2_tid, "timeStamp", HOFFSET(Event, timeStamp), H5T_NATIVE_ULLONG);
    H5Tinsert(s2_tid, "data", HOFFSET(Event, data), H5T_NATIVE_CHAR);

    /* Create a memory dataspace to hold the chunk data */
    hsize_t slab_start[] = {0};
    hsize_t slab_size[] = {CHUNK_SIZE};
    hid_t memspace = H5Screate_simple(1, slab_size, NULL);
    H5Sselect_hyperslab(memspace, H5S_SELECT_SET, slab_start, NULL, slab_size, NULL);

    /* 
    * Read the first chunk
    */
    hsize_t offset[] = {hsize_t(chunksToRetrieve.first * CHUNK_SIZE)};
    if(chunksToRetrieve.first == ndims - 1){
        slab_size[0] = {dims_out[0]-(chunksToRetrieve.first*CHUNK_SIZE) };
        memspace = H5Screate_simple(1, slab_size, NULL);
        H5Sselect_hyperslab(memspace, H5S_SELECT_SET, slab_start, NULL, slab_size, NULL);
    }

    H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, NULL, slab_size, NULL);
    status = H5Dread(dataset, s2_tid, memspace, dataspace, H5P_DEFAULT, eventresult);

    for(hsize_t i = 0 ; i < slab_size[0]; i++ ){
        if(eventresult[i].timeStamp >= range.first && eventresult[i].timeStamp <= range.second){
            r1.push_back(eventresult[i]);
        }
    }
    free(eventresult);


    /* 
    * Read the intermediate chunks
    */
    for(hsize_t i = chunksToRetrieve.first + 1 ; i < chunksToRetrieve.second ; i++){
        offset[0] = {hsize_t(i * CHUNK_SIZE)};
        eventresult = (Event*) malloc(CHUNK_SIZE * sizeof(Event));
        H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, NULL, slab_size, NULL);
        status = H5Dread(dataset, s2_tid, memspace, dataspace, H5P_DEFAULT, eventresult);
        for(hsize_t i = 0 ; i < slab_size[0]; i++ ){
            r1.push_back(eventresult[i]);
        }
        free(eventresult);
    }

    /* 
    * Read the last chunk
    */
   if(chunksToRetrieve.first != chunksToRetrieve.second){    // If both the chunks to read are same do not read chunk
        if(chunksToRetrieve.second == ndims - 1){
            slab_size[0] = {dims_out[0]-(chunksToRetrieve.second*CHUNK_SIZE) };
            memspace = H5Screate_simple(1, slab_size, NULL);
            H5Sselect_hyperslab(memspace, H5S_SELECT_SET, slab_start, NULL, slab_size, NULL);
        }
        offset[0] = {hsize_t(chunksToRetrieve.second * CHUNK_SIZE)};
        eventresult = (Event*) malloc(CHUNK_SIZE * sizeof(Event));
        H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, NULL, slab_size, NULL);
        status = H5Dread(dataset, s2_tid, memspace, dataspace, H5P_DEFAULT, eventresult);

        for(hsize_t i = 0 ; i < slab_size[0]; i++ ){
            if(eventresult[i].timeStamp <= range.second){
                r1.push_back(eventresult[i]);
            }
            else{
                break;
            }
        }
        free(eventresult);
   }


    H5Sclose(memspace);
    free(attribute_data);
    // free(eventresult);
    H5Aclose(attribute_id);
    H5Tclose(attribute_type);
    // free(eventBuffer);
    H5Tclose(s2_tid);
    H5Dclose(dataset);
    H5Fclose(file);
    
    return r1;
}