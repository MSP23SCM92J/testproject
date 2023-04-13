#include<iostream>
#include<hdf5.h>

int writeToDataset(const char* FILE_NAME, const char* DATASET_NAME, const char* H5FILE_NAME, hsize_t CHUNK_SIZE);