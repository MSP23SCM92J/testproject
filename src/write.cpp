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
#define ATTR_CHUNKMETADATA "ChunkMetadata"
#define ATTR_MIN "Min"
#define ATTR_MAX "Max"
#define CHUNK_META_ATTR_RANK 1
#define MIN_ATTR_RANK 1
#define MAX_ATTR_RANK 1

// Write data to H5 dataset. returns status as 0 if write is successful else returns 1
int writeToDataset(const char* FILE_NAME, const char* DATASET_NAME, const char* H5FILE_NAME, int64_t CHUNK_SIZE) {

    hid_t   file, dataset, space, cspace, dcpl_id, mspace, s1_tid;
    herr_t  status;
    hsize_t chunk_dims[1] = {CHUNK_SIZE};
    std::vector<Event> temp;
    int64_t time;
    std::string da;

    std::ifstream filePointer(FILE_NAME);
    filePointer.seekg(0, std::ios::end);
    std::streampos fileSize = filePointer.tellg();
    filePointer.seekg(0, std::ios::beg);

    /*
     * Raw number of events
     */
    hsize_t numEvents = fileSize / (sizeof(int64_t)+(sizeof(char) * 100));

    std::cout<<"Raw number of events: "<<numEvents<<"\n";
    /*
     * Allocate memory for events data
     */
    Event* eventBuffer = (Event*) malloc(numEvents*sizeof(Event));
    // std::cout<<"Allocate memory for events data: "<<numEvents*sizeof(Event)<<"\n";
    /*
     * Read data from $FILE_NAME file
     */
    unsigned long long iterator = 0;
    while (filePointer >> time >> da) {
        // Event e;
        // e.timeStamp = time;
        // strcpy(e.data, da.c_str());
        // temp.push_back(e);
        eventBuffer[iterator].timeStamp = time;
        strcpy(eventBuffer[iterator].data, da.c_str());
        // std::cout<<time<<" "<<da<<"\n";
        iterator++;
    }
    filePointer.close();

    // Event* eventBuffer = (Event*) malloc(iterator * sizeof(Event));
    // std::cout<<"Allocate memory for events data: "<<iterator*sizeof(Event)<<"\n";
    // for(int i=0 ; i<temp.size() ; i++){
    //     eventBuffer[i].timeStamp = temp[i].timeStamp;
    //     // eventBuffer[i].data = temp[i].data;
    //     strcpy(eventBuffer[i].data, temp[i].data);
    // }
    // temp.erase(temp.begin(), temp.end());

    // std::cout<<"Read data from $FILE_NAME file"<<"\n";
    /*
     * Create the file
     */
    const hsize_t numberOfEvents = iterator - 1;
    std::cout<<"numberOfEvents: "<<numberOfEvents<<"\n";
    hsize_t dim[] = {numberOfEvents};
    const hsize_t max_dim[] = {H5S_UNLIMITED};
    file = H5Fcreate(H5FILE_NAME, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

    /*
     * Enable chunking and chunk caching for the datset
     */
    dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
    status = H5Pset_chunk(dcpl_id, 1, chunk_dims);

    // Set up the chunk cache
    // hsize_t chunk_cache_size = 10000;
    // size_t rdcc_nslots = chunk_cache_size / chunk_dims[0];
    // size_t rdcc_nbytes = chunk_cache_size * sizeof(Event);
    // double rdcc_w0 = 0.75;

    // H5Pset_chunk_cache(dcpl_id, rdcc_nslots, rdcc_nbytes, rdcc_w0);
    // delete[] rdcc_w0;

    /*
     * Create the data space.
     */
    space = H5Screate_simple(DATASET_RANK, dim, max_dim );
    std::cout<<"Create the data space."<<"\n";
    /*
     * Create the memory data type.
     */
    s1_tid = H5Tcreate(H5T_COMPOUND, sizeof(Event));
    H5Tinsert(s1_tid, "timeStamp", HOFFSET(Event, timeStamp), H5T_NATIVE_ULONG);
    H5Tinsert(s1_tid, "data", HOFFSET(Event, data), H5T_NATIVE_CHAR);

    /*
     * Create the dataset.
     */
    dataset = H5Dcreate2(file, DATASET_NAME, s1_tid, space, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);

    /*
     * Allocate memory for events data
     */
    ChunkAttr* attrBuffer = (ChunkAttr*) malloc(((iterator % CHUNK_SIZE)+1)*sizeof(ChunkAttr));
    std::cout<<"Allocate memory for events data"<<"\n";
    /*
     * Write data to the dataset using hyperslab
     */
    hsize_t slab_start[] = {0};
    hsize_t space_start[] = {0};
    hsize_t slab_count[1] = {CHUNK_SIZE};
    mspace = H5Screate_simple(DATASET_RANK, slab_count, NULL);
    status = H5Sselect_hyperslab(mspace, H5S_SELECT_SET, slab_start, NULL, slab_count, NULL);
    if(status != 0){
        std::cout<<"Error: "<<status<<"\n";
    }
    std::cout<<"Write data to the dataset using hyperslab"<<"\n";
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
        status = H5Sselect_hyperslab(space, H5S_SELECT_SET, space_start, NULL, slab_count, NULL);
        status = H5Dwrite(dataset, s1_tid, mspace, space, H5P_DEFAULT, &eventBuffer[i]);

        attrBuffer[attrcount].start = eventBuffer[i].timeStamp;
        attrBuffer[attrcount].end = eventBuffer[i + slab_count[0] - 1].timeStamp;
        std::cout<<attrBuffer[attrcount].start <<" "<<attrBuffer[attrcount].end<<std::endl;
        attrcount++;
    }
    std::cout<<"Written data"<<"\n";
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
    std::cout<<"Create dataspace for the ChunkMetadata attribute."<<"\n";
    /*
     * Create array attribute and write array attribute.
     */
    hid_t attr1 = H5Acreate2(dataset, ATTR_CHUNKMETADATA, at_tid, attr, H5P_DEFAULT, H5P_DEFAULT);
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
    free(attrBuffer);
    free(eventBuffer);
    H5Sclose(mspace);
    H5Pclose(dcpl_id);
    H5Aclose(attr1);
    H5Aclose(min_attr);
    H5Aclose(max_attr);
    H5Tclose(s1_tid);
    H5Sclose(space);
    H5Dclose(dataset);
    H5Fclose(file);

    return 0;
}

// Append to existing dataset
int appendToDataset(const char* FILE_NAME, const char* DATASET_NAME, const char* H5FILE_NAME) {

    hid_t   file, dataset, space, mspace, s1_tid;
    herr_t  status;
    hsize_t chunk_size[1];

    int64_t time;
    std::string da;

    std::ifstream filePointer(FILE_NAME);
    filePointer.seekg(0, std::ios::end);
    std::streampos fileSize = filePointer.tellg();
    filePointer.seekg(0, std::ios::beg);

    hsize_t numEvents = fileSize/(sizeof(int64_t)+(sizeof(char)*100));

    /*
     * Allocate memory for events data
     */
    Event* eventBuffer = (Event*) malloc(numEvents*sizeof(Event));

    /*
     * Read data from $FILE_NAME file
     */
    __int64_t iterator = 0;
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

    /*
     * Create the memory data type.
     */
    s1_tid = H5Tcreate(H5T_COMPOUND, sizeof(Event));
    H5Tinsert(s1_tid, "timeStamp", HOFFSET(Event, timeStamp), H5T_NATIVE_ULONG);
    H5Tinsert(s1_tid, "data", HOFFSET(Event, data), H5T_NATIVE_CHAR);

    file = H5Fopen(H5FILE_NAME, H5F_ACC_RDWR, H5P_DEFAULT);
    dataset = H5Dopen2(file, DATASET_NAME, H5P_DEFAULT);

    hid_t plist_id = H5Dget_create_plist(dataset);
    int rank = H5Pget_chunk(plist_id, 1, NULL);
    H5Pget_chunk(plist_id, 1, chunk_size);

    /*
     * Get the existing size of the dataset
     */ 
    hid_t dataspace_id = H5Dget_space(dataset);
    hsize_t current_size[1];
    H5Sget_simple_extent_dims(dataspace_id, current_size, NULL);

    /*
     * Define the new size of the dataset and extend it
     */
    hsize_t new_size[1] = {current_size[0] + dim[0]};
    status = H5Dset_extent(dataset, new_size);
    
    
    H5Sclose(dataspace_id);
    H5Dclose(dataset);

    dataset = H5Dopen2(file, DATASET_NAME, H5P_DEFAULT);
    dataspace_id = H5Dget_space(dataset);
    hsize_t _size[1];
    H5Sget_simple_extent_dims(dataspace_id, _size, NULL);
    
    /*
     * Allocate memory for events data
     */
    ChunkAttr* attrBuffer = (ChunkAttr*) malloc(((iterator % chunk_size[0]) + 1) * sizeof(ChunkAttr));

    /*
     * Write data to the dataset using hyperslab
     */
    hsize_t slab_start[] = {0};
    hsize_t space_start[] = {current_size[0]};
    hsize_t slab_count[1] = {chunk_size[0]};
    mspace = H5Screate_simple(DATASET_RANK, slab_count, NULL);
    status = H5Sselect_hyperslab(mspace, H5S_SELECT_SET, slab_start, NULL, slab_count, NULL);

    // TODO: check the last entry issue
    hsize_t attrcount = 0;
    hsize_t i = 0;
    hsize_t datasize = 0;
    for (i = current_size[0] ; i < new_size[0] ; i += chunk_size[0]) {
        space_start[0] = i;
        if(new_size[0] - i < chunk_size[0]){
            slab_count[0] = new_size[0] - i;

            // TODO: check mspace declaration
            mspace = H5Screate_simple(DATASET_RANK, slab_count, NULL);
            status = H5Sselect_hyperslab(mspace, H5S_SELECT_SET, slab_start, NULL, slab_count, NULL);

        }

        status = H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, space_start, NULL, slab_count, NULL);
        status = H5Dwrite(dataset, s1_tid, mspace, dataspace_id, H5P_DEFAULT, &eventBuffer[datasize]);
        
        /*
         * Add chunk's min and max to attribute table(data)
         */
        attrBuffer[attrcount].start = eventBuffer[datasize].timeStamp;
        attrBuffer[attrcount].end = eventBuffer[datasize + slab_count[0] - 1].timeStamp;
        datasize += chunk_size[0];
        attrcount++;
    }
    free(eventBuffer);

    /*
     * Create dataspace for the ChunkMetadata attribute.
     */
    hid_t at_tid = H5Tcreate(H5T_COMPOUND, sizeof(ChunkAttr));
    H5Tinsert(at_tid, "start", HOFFSET(ChunkAttr, start), H5T_NATIVE_INT64);
    H5Tinsert(at_tid, "end", HOFFSET(ChunkAttr, end), H5T_NATIVE_INT64);

    /*
     * Open the attribute, get attribute id, type and space
     */
    hid_t attribute_id = H5Aopen(dataset, ATTR_CHUNKMETADATA, H5P_DEFAULT);
    hid_t attribute_type = H5Aget_type(attribute_id);
    hid_t dataspace_a_id = H5Aget_space(attribute_id);
    hsize_t ndims = H5Sget_simple_extent_npoints(dataspace_a_id);
    H5Sclose(dataspace_a_id);

    hsize_t adim[] = {ndims + attrcount};
    ChunkAttr* attribute_data = (ChunkAttr *) malloc(adim[0] * sizeof(ChunkAttr));

    /*
     * Read the attribute data into the buffer and delete the attribute
     */
    H5Aread(attribute_id, attribute_type, attribute_data);
    H5Aclose(attribute_id);
    H5Adelete(dataset, ATTR_CHUNKMETADATA);
    
    hid_t attr = H5Screate(H5S_SIMPLE);
    herr_t ret  = H5Sset_extent_simple(attr, CHUNK_META_ATTR_RANK, adim, NULL);
    hid_t attr1 = H5Acreate2(dataset, ATTR_CHUNKMETADATA, attribute_type, attr, H5P_DEFAULT, H5P_DEFAULT);

    for(int i = 0 ; i < attrcount ; i++ ){
        attribute_data[ndims+i].start = attrBuffer[i].start;
        attribute_data[ndims+i].end = attrBuffer[i].end;
    }
    ret = H5Awrite(attr1, attribute_type, attribute_data);

    /*
     * Modify MAX attribute
     */
    hid_t attr_id = H5Aopen(dataset, ATTR_MAX, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_INT64, &attribute_data[adim[0] - 1].end);

    /*
     * Release resources
     */
    free(attrBuffer);
    free(attribute_data);
    H5Sclose(mspace);
    H5Aclose(attr_id);
    H5Aclose(attr1);
    H5Tclose(s1_tid);
    H5Dclose(dataset);
    H5Fclose(file);

    return 0;
}