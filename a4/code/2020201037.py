#!/bin/python3
import sys
import subprocess
import os
inputR = sys.argv[1]
inputS = sys.argv[2]
M = sys.argv[3]
method = sys.argv[4]

if method == "sort":
    print("sort")
    # print(os.getcwd())
    subprocess.getoutput("code/sort_merge_join.py " +inputR+ " "+ inputS+" "+M )

elif method == "hash":
    subprocess.getoutput("code/hash_join.py " +inputR+ " "+ inputS+" "+M )
else: print("wrong method")



