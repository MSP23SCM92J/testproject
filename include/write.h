#include<iostream>
#ifndef WRITE_H
#define WRITE_H

// Write data to H5 dataset
int writeToDataset(const char* FILE_NAME, const char* DATASET_NAME, const char* H5FILE_NAME, int64_t CHUNK_SIZE);

// Append to existing dataset
int appendToDataset(const char* FILE_NAME, const char* DATASET_NAME, const char* H5FILE_NAME);

#endif