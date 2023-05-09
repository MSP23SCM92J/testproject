#include <iostream>
#include <string>
#include <vector>
#include <hdf5.h>
#include <read.h>
#include <event.h>
#include <chunkattr.h>

#define ATTR_CHUNKMETADATA "ChunkMetadata"
#define ATTR_MIN "Min"
#define ATTR_MAX "Max"
#define DIMENSION 1
#define DEBUG 0

// Helper function
int64_t binarySearch(ChunkAttr* data, __uint64_t target, __uint64_t dataSize) {
    __uint64_t left = 0;
    __uint64_t right = dataSize - 1;
    __uint64_t mid = -1;
    while (left <= right) {
        mid = left + (right - left) / 2;
        if (data[mid].start <= target && target <= data[mid].end) {
            break;
        }
        else if (data[mid].start > target) {
            right = mid - 1;
        }
        else {
            left = mid + 1;
        }
    }
    return mid;
}

// Read data from H5 dataset returns empty vector if range not within limits else returns vector filled with Events data
std::vector<Event> readFromDataset(const char* DATASET_NAME, std::pair<__uint64_t, __uint64_t> range, const char* H5FILE_NAME){
    
    // Disable automatic printing of HDF5 error stack
    if(!DEBUG){
        H5Eset_auto(H5E_DEFAULT, NULL, NULL);
    }

    std::vector<Event> resultEvents;
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

    /*
     * Open the attribute
     * Get the attribute type and dataspace
     * Get the number of dimensions of the dataspace
     * Get the dataset creation property list for chunksize information
     * Read the attribute data into the buffer
     */
    hid_t attribute_id = H5Aopen(dataset, ATTR_CHUNKMETADATA, H5P_DEFAULT);

    hid_t attribute_type = H5Aget_type(attribute_id);
    hid_t dataspace_id = H5Aget_space(attribute_id);

    hsize_t ndims = H5Sget_simple_extent_npoints(dataspace_id);

    hid_t plist_id = H5Dget_create_plist(dataset);
    hsize_t chunk_dims[1];
    H5Pget_chunk(plist_id, 1, chunk_dims);
    long long int CHUNK_SIZE = chunk_dims[0];
    ChunkAttr* attribute_data = (ChunkAttr *) malloc(ndims * sizeof(ChunkAttr));
    H5Aread(attribute_id, attribute_type, attribute_data);


    std::pair<int64_t, int64_t> chunksToRetrieve = {-1 , -1};
    hsize_t i = 0;

    /*
     * Find the starting chunk index
     */
    // for(i=0 ; i < ndims ; i++){
    //     if(attribute_data[i].start <= range.first && attribute_data[i].end >= range.first)
    //     {
    //         chunksToRetrieve.first = i;
    //         break;
    //     }
    // }

    // /*
    //  * Find the ending chunk index
    //  */
    // for( ; i < ndims ; i++){
    //     if(attribute_data[i].start <= range.second && attribute_data[i].end >= range.second)
    //     {
    //         chunksToRetrieve.second = i;
    //         break;
    //     }
    // }

    chunksToRetrieve.first = binarySearch(attribute_data, range.first, ndims);
    chunksToRetrieve.second = binarySearch(attribute_data, range.second, ndims);


    /*
     * Check for the range of requested query of chunks are not found
     */
    if(chunksToRetrieve.second == -1 || chunksToRetrieve.first == -1){
        H5Dclose(dataset);
        H5Fclose(file);
        H5Aclose(attribute_id);
        H5Tclose(attribute_type);
        return resultEvents;
    }
    
    Event* eventresult = (Event*) malloc(CHUNK_SIZE * sizeof(Event));

    s2_tid = H5Tcreate(H5T_COMPOUND, sizeof(Event));
    H5Tinsert(s2_tid, "timeStamp", HOFFSET(Event, timeStamp), H5T_NATIVE_ULLONG);
    H5Tinsert(s2_tid, "data", HOFFSET(Event, data), H5T_NATIVE_CHAR);

    /* 
     * Create a memory dataspace to hold the chunk data 
     */
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
            resultEvents.push_back(eventresult[i]);
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
            resultEvents.push_back(eventresult[i]);
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
                resultEvents.push_back(eventresult[i]);
            }
            else{
                break;
            }
        }
        free(eventresult);
   }


    /* 
    * Release resources
    */
    H5Sclose(memspace);
    free(attribute_data);
    H5Aclose(attribute_id);
    H5Tclose(attribute_type);
    H5Tclose(s2_tid);
    H5Dclose(dataset);
    H5Fclose(file);
    
    return resultEvents;
}

// Read dataset data range i.e Min and Max
std::pair<__uint64_t, __uint64_t> readDatasetRange(const char* DATASET_NAME, const char* H5FILE_NAME){

    std::pair<__uint64_t, __uint64_t> datasetRange;
    hid_t   file, dataset, space, dataspace, attribute_id, attribute_did;
    hsize_t dims_out[DIMENSION];
    herr_t  status;
    __uint64_t attribute_value;

    /*
     * Open H5 file and dataset
     */
    file = H5Fopen(H5FILE_NAME, H5F_ACC_RDONLY, H5P_DEFAULT);
    dataset = H5Dopen2(file, DATASET_NAME, H5P_DEFAULT);
    dataspace  = H5Dget_space(dataset);
    status = H5Sget_simple_extent_dims(dataspace, dims_out, NULL);

    /*
     * Open and read Min attribute
     */
    attribute_did = H5Aopen(dataset, ATTR_MIN, H5P_DEFAULT);
    hid_t dataspace_id = H5Aget_space(attribute_did);
    status = H5Aread(attribute_did, H5T_NATIVE_UINT64, &datasetRange.first);

    /*
     * Open and read Max attribute
     */
    attribute_did = H5Aopen(dataset, ATTR_MAX, H5P_DEFAULT);
    dataspace_id = H5Aget_space(attribute_did);
    status = H5Aread(attribute_did, H5T_NATIVE_UINT64, &datasetRange.second);

    /*
     * Release resources
     */
    H5Aclose(attribute_did);
    H5Sclose(dataspace_id);
    H5Dclose(dataset);
    H5Fclose(file);

    return datasetRange;
}