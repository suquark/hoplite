def create_store_using_dict(args_dict):
    import py_distributed_object_store as store_lib
    store = store_lib.DistributedObjectStore(
        args_dict['redis_address'],
        args_dict['redis_port'],
        args_dict['notification_port'],
        args_dict['notification_listening_port'],
        args_dict['plasma_socket'],
        args_dict['object_writer_port'],
        args_dict['grpc_port'])
    return store


def object_id_from_int(n):
    import py_distributed_object_store as store_lib
    return store_lib.ObjectID(str(n).encode().rjust(20, b'\0'))
