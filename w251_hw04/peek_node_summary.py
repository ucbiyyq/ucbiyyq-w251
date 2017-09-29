import pickle
import numpy as np
import pandas as pd

gpfs_suffix = "gpfs1"
gpfs_prefix = "/gpfs/gpfsfpo/"
gpfs_counts_fullpath = gpfs_prefix + gpfs_suffix + "/counts/counts.pkl"

def peek_node_summary():
    frame = None
    with open(gpfs_counts_fullpath, "rb") as p:
        temp = pickle.load(p)
        frame = temp
    print(frame.head())

peek_node_summary()
