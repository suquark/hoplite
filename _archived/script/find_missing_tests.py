import os
import sys

def main(log_dir):
    files = os.listdir(log_dir)

    tasks =  {'multicast', 'reduce', 'allreduce'}
    node_set = range(2, 18, 2)
    object_size_set = {2**i for i in range(20, 31)}

    print (node_set)
    print (object_size_set)

    for task_name in tasks: 
        for number_of_nodes in node_set:
            for object_size in object_size_set:
                task = task_name + '-' + str(number_of_nodes) + '-' + str(object_size)
                found = False
                for filename in files:
                    if task in filename:
                        found = True
                        break
                if not found:
                    print (task)


if __name__ == "__main__":
    assert len(sys.argv) == 2, "Usage: python parse_mpi_result.py LOG_DIR"
    log_dir = sys.argv[1]
    main(log_dir)
