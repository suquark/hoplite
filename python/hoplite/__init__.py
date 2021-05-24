import atexit
import pathlib
import subprocess
import time

from . import _hoplite_client as _hoplite_store

HopliteClient = _hoplite_store.DistributedObjectStore
Buffer = _hoplite_store.Buffer
ObjectID = _hoplite_store.ObjectID
ReduceOp = _hoplite_store.ReduceOp


def get_my_address():
    import socket
    return socket.gethostbyname(socket.gethostname())


def add_arguments(parser):
    parser.add_argument('--redis-address', type=str, default=get_my_address(),
                        help='The IP address of the redis server')
    parser.add_argument('--redis-port', type=int, default=6380,
                        help='The port of the redis server')
    parser.add_argument('--notification-port', type=int, default=7777,
                        help='The port of the notification server')
    parser.add_argument('--notification-listening-port', type=int, default=8888,
                        help='The listening port of the notification client')
    parser.add_argument('--plasma-socket', type=str, default="/tmp/multicast_plasma",
                        help='The path of the unix domain socket')
    parser.add_argument('--object_writer_port', type=int, default=6666,
                        help='The path of the unix domain socket')
    parser.add_argument('--grpc-port', type=int, default=50055,
                        help='The path of the unix domain socket')


def extract_dict_from_args(args):
    return {'redis_address': args.redis_address.encode()}


def create_store_using_dict(args_dict):
    store = _hoplite_store.DistributedObjectStore(args_dict['redis_address'])
    return store


def object_id_from_int(n):
    return _hoplite_store.ObjectID(int(str(n), 16).to_bytes(20, byteorder='big'))


def random_object_id():
    import random
    return object_id_from_int(random.randint(0, 1e20-1))


def _register_cleanup(processes):
    def _cleanup_processes():
        print("Cleaning up process...")
        # wait clients to exit to suppress error messages
        time.sleep(0.5)
        for p in processes:
            p.terminate()
    atexit.register(_cleanup_processes)


def start_location_server():
    server_exec = pathlib.Path(__file__).resolve().parent.absolute() / 'notification'
    notification_p = subprocess.Popen([str(server_exec)])
    _register_cleanup([notification_p])
    time.sleep(2)
    return get_my_address()


__all__ = ('start_location_server', 'random_object_id', 'object_id_from_int',
           'create_store_using_dict', 'extract_dict_from_args', 'add_arguments', 'get_my_address',
           'Buffer', 'ObjectID', 'ReduceOp')
