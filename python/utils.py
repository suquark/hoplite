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


def create_store_using_args(args):
    import py_distributed_object_store as store_lib
    store = store_lib.DistributedObjectStore(
        args.redis_address.encode(), args.redis_port,
        args.notification_port, args.notification_listening_port,
        args.plasma_socket.encode(),
        get_my_address(), args.object_writer_port,
        args.grpc_port)
    return store
