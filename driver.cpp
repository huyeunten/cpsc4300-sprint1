using namespace std;
#include "heap_storage.h"
#include "storage_engine.h"

int main(){
    HeapFile* file = new HeapFile("file.db");
    file->open();
    file->close();
    file->drop();
    return 0;
}