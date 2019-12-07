object_store: object_store.cc
	g++ -o object_store object_store.cc `pkg-config --cflags --libs plasma` --std=c++11

clean:
	rm object_store
