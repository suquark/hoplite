#!/bin/bash
make clean
clang-format -i *.cc src/*.cc src/*.h src/util/*.cc src/util/*.h
