#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstring>
#include <vector>
#include <event.h>
#include <hdf5.h>
#include <chrono>
#include <chunkattr.h>

#define DATASET_RANK 1
#define CHUNK_SIZEE 1000
#define ATTR_CHUNK_METADATA "ChunkMetadata"
#define ATTR_MIN "Min"
#define ATTR_MAX "Max"
#define CHUNK_META_ATTR_RANK 1
#define MIN_ATTR_RANK 1
#define MAX_ATTR_RANK 1

// Write data to H5 datast. returns status as 0 if write is successful else returns 1
int writeToDataset(const char* FILE_NAME, const char* DATASET_NAME, const char* H5FILE_NAME, hsize_t CHUNK_SIZE) {

    hid_t   file, dataset, space, cspace, dpcl_id, mspace, s1_tid;
    herr_t  status;
    hsize_t chunk_dims[1] = {CHUNK_SIZE};

    int64_t time;
    std::string da;

    std::ifstream filePointer(FILE_NAME);
    filePointer.seekg(0, std::ios::end);
    std::streampos fileSize = filePointer.tellg();
    filePointer.seekg(0, std::ios::beg);

    // raw number of events
    hsize_t numEvents = fileSize/(sizeof(int64_t)+(sizeof(char)*100));

    // Allocate memory for events data
    Event* eventBuffer = (Event*) malloc(numEvents*sizeof(Event));

    // read data from $FILE_NAME file
    unsigned long long iterator = 0;
    while (filePointer >> time >> da) {
        eventBuffer[iterator].timeStamp = time;
        strcpy(eventBuffer[iterator].data, da.c_str());
        iterator++;
    }
    filePointer.close();

    /*
     * Create the file
     */
    const hsize_t numberOfEvents = iterator - 1;
    hsize_t dim[] = {numberOfEvents};
    file = H5Fcreate(H5FILE_NAME, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

    dpcl_id = H5Pcreate(H5P_DATASET_CREATE);
    status = H5Pset_chunk(dpcl_id, 1, chunk_dims);

    /*
     * Create the data space.
     */
    space = H5Screate_simple(DATASET_RANK, dim, NULL);

    /*
     * Create the memory data type.
     */
    s1_tid = H5Tcreate(H5T_COMPOUND, sizeof(Event));
    H5Tinsert(s1_tid, "timeStamp", HOFFSET(Event, timeStamp), H5T_NATIVE_ULONG);
    H5Tinsert(s1_tid, "data", HOFFSET(Event, data), H5T_NATIVE_CHAR);

    /*
     * Create the dataset.
     */
    dataset = H5Dcreate2(file, DATASET_NAME, s1_tid, space, H5P_DEFAULT, dpcl_id, H5P_DEFAULT);

    // Allocate memory for events data
    ChunkAttr* attrBuffer = (ChunkAttr*) malloc(((iterator % CHUNK_SIZE)+1)*sizeof(ChunkAttr));

    /*
     * Write data to the dataset using hyperslab
     */
    hsize_t slab_start[] = {0};
    hsize_t space_start[] = {0};
    hsize_t slab_count[1] = {CHUNK_SIZE};
    mspace = H5Screate_simple(DATASET_RANK, slab_count, NULL);
    status = H5Sselect_hyperslab(mspace, H5S_SELECT_SET, slab_start, NULL, slab_count, NULL);

    // TODO: check the last entry issue
    hsize_t attrcount = 0;
    hsize_t i = 0;
    for (i = 0; i < dim[0]; i += CHUNK_SIZE) {
        space_start[0] = i;
        if(dim[0] - i < CHUNK_SIZE){
            slab_count[0] = dim[0] - i;
            // TODO: check mspace declaration
            mspace = H5Screate_simple(DATASET_RANK, slab_count, NULL);
            status = H5Sselect_hyperslab(mspace, H5S_SELECT_SET, slab_start, NULL, slab_count, NULL);
        }
        else{
            status = H5Sselect_hyperslab(space, H5S_SELECT_SET, space_start, NULL, slab_count, NULL);
        }

        status = H5Sselect_hyperslab(space, H5S_SELECT_SET, space_start, NULL, slab_count, NULL);
        status = H5Dwrite(dataset, s1_tid, mspace, space, H5P_DEFAULT, &eventBuffer[i]);

        attrBuffer[attrcount].start = eventBuffer[i].timeStamp;
        attrBuffer[attrcount].end = eventBuffer[i + slab_count[0] - 1].timeStamp;
        attrcount++;
    }

    /*
     * Create dataspace for the ChunkMetadata attribute.
     */
    hid_t at_tid = H5Tcreate(H5T_COMPOUND, sizeof(ChunkAttr));
    H5Tinsert(at_tid, "start", HOFFSET(ChunkAttr, start), H5T_NATIVE_INT64);
    H5Tinsert(at_tid, "end", HOFFSET(ChunkAttr, end), H5T_NATIVE_INT64);
    hid_t attr;
    hsize_t adim[] = {attrcount};

    attr = H5Screate(H5S_SIMPLE);
    herr_t ret  = H5Sset_extent_simple(attr, CHUNK_META_ATTR_RANK, adim, NULL);

    /*
     * Create array attribute and write array attribute.
     */
    hid_t attr1 = H5Acreate2(dataset, ATTR_CHUNK_METADATA, at_tid, attr, H5P_DEFAULT, H5P_DEFAULT);
    ret = H5Awrite(attr1, at_tid, attrBuffer);

    /*
     * Create and set Min and Max attribute.
     */
    at_tid = H5Tcreate(H5T_COMPOUND, sizeof(ChunkAttr));
    adim[0] = {1};
    attr = H5Screate_simple(1, adim, NULL);

    hid_t min_attr = H5Acreate2(dataset, ATTR_MIN, H5T_NATIVE_INT64, attr, H5P_DEFAULT, H5P_DEFAULT);
    ret = H5Awrite(min_attr, H5T_NATIVE_INT64, &eventBuffer[0].timeStamp);

    hid_t max_attr = H5Acreate2(dataset, ATTR_MAX, H5T_NATIVE_INT64, attr, H5P_DEFAULT, H5P_DEFAULT);
    ret = H5Awrite(max_attr, H5T_NATIVE_INT64, &eventBuffer[dim[0]-1].timeStamp);

    /*
     * Release resources
     */
    free(eventBuffer);
    H5Sclose(mspace);
    H5Pclose(dpcl_id);
    H5Aclose(attr1);
    H5Aclose(min_attr);
    H5Aclose(max_attr);
    H5Tclose(s1_tid);
    H5Sclose(space);
    H5Dclose(dataset);
    H5Fclose(file);

    return 0;
}
