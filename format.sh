#!/bin/bash
make clean
clang-format -i *.cc *.h util/*.cc util/*.h
