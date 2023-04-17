#include<iostream>
#include<hdf5.h>

// Write data to H5 dataset
int writeToDataset(const char* FILE_NAME, const char* DATASET_NAME, const char* H5FILE_NAME, hsize_t CHUNK_SIZE);

// Append to existing dataset
int appendToDataset(const char* FILE_NAME, const char* DATASET_NAME, const char* H5FILE_NAME);