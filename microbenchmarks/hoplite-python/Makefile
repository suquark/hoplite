all:
	python -m pip install grpcio-tools
	python -m grpc_tools.protoc -I../../src/protocol/ --python_out=. --grpc_python_out=. object_store.proto
	ln -sfn $(realpath -s ../../python/hoplite/) hoplite

clean:
	rm object_store_pb2*.py
