all: object_store_pb2_grpc

object_store_pb2_grpc.py:
	python -m pip install grpcio-tools
	python -m grpc_tools.protoc -I../../src/protocol/ --python_out=. --grpc_python_out=. object_store.proto

clean:
	rm object_store_pb2*.py
