import ray

@ray.remote(resources={'node': 1}, max_calls=1)
def check_env():
    import socket
    import sys
    print(socket.gethostbyname(socket.gethostname()), sys.path)