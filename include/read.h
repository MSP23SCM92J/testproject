#include <iostream>
#include <event.h>

std::vector<Event> readFromDataset(const char* DATSET_NAME, std::pair<int64_t, int64_t> range, const char* H5FILE_NAME);