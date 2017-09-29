import sys
import pickle
import numpy as np
import pandas as pd

def main():
    gpfs_counts_fullpath = sys.argv[1]
    nrows = int(sys.argv[2])
    
    with open(gpfs_counts_fullpath, "rb") as p:
        temp = pickle.load(p)
        print(temp.head(n=nrows))

if __name__ == "__main__":
    main()