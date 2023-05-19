#include <iostream>
#include <testReadWrite.h>

int main(int argc, char** argv) {

    // Write to dataset
    // testWriteOperation();

    // Retrieve story dataset min and max timestamp
    // testStoryRange();

    // Sequential reads
    testReadOperation();

    return 0;
}
