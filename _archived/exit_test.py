#!/usr/bin/env python3
import argparse
import time

import py_distributed_object_store as store_lib
import utils

parser = argparse.ArgumentParser()
utils.add_arguments(parser)

args = parser.parse_args()
args_dict = utils.extract_dict_from_args(args)

store = utils.create_store_using_dict(args_dict)
time.sleep(5)
print ("Exiting")

