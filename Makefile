m: milestone1.o
	g++ -L/usr/local/db6/lib -o $@ $< -ldb_cxx -lsqlparser

milestone1.o: milestone1.cpp heap_storage.o
	g++ -I/usr/local/db6/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

heap_storage.o: heap_storage.cpp heap_storage.h storage_engine.h
	g++ -I/usr/local/db6/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

clean: 
	rm -f milestone1.o m