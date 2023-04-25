#include<iostream>
#ifndef CHUNK_ATTR_H
#define CHUNK_ATTR_H


typedef struct ChunkAttr {
    __uint64_t start;
    __uint64_t end;
}ChunkAttr;

#endif // CHUNK_ATTR_H