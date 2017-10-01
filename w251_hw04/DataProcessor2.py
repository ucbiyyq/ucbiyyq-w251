import io
import os
import re
import sys
import csv
import zipfile
import numpy as np
import pandas as pd

class DataProcessor2(object):
    '''
    Class: DataProcessor
    
    Helper class that handles the details of processing the data files.
    Intended to be used by one of the prepare-data scripts on the local gpfs node.
    '''

    def __init__(self, gpfs_suffix):
        self.gpfs_suffix = gpfs_suffix
        self.gpfs_prefix = "/gpfs/gpfsfpo/"
        self.file_prefix = "googlebooks-eng-all-2gram-20090715-"
        self.gpfs_path = self.gpfs_prefix + self.gpfs_suffix + "/"
        self.file_headers = ["bi_gram", "year", "match_count", "page_count", "volume_count"]
        self.chunk_size = 1000000

        
    def prep_file(self, file_suffix, nrows=None):
        '''
        Given a zip file suffix, constructs an output file of the bi-gram and its match-counts, e.g.
        
               bi_gram_0 bi_gram_1  match_count
            0  financial analysis   130
            1  financial capacity   75
            2  financial straits    53
            3  ...
            4  ...
        
        Format of the output file is 
            bi_gram_0 TAB bi_gram_1 TAB match_count
        
        Name of the output file will be the similar, with an "-count.csv" suffix, e.g.
            input file: /gpfs/gpfsfpo/gpfs1/googlebooks-eng-all-2gram-20090715-0.csv.zip
            output file: /gpfs/gpfsfpo/gpfs1/googlebooks-eng-all-2gram-20090715-0-count.csv 
        
        Google's description indicates the ngrams inside each file are sorted alphabetically and then chronologically. 
        But, the files themselves aren't necesarily ordered with respect to one another. 
        e.g. A French two word phrase starting with 'm' will be in the middle of one of the French 2-gram files, 
        but there's no way to know which without checking them all.
        '''
        
        
        def is_useful(reo, word):
            '''
            internal helper function to determine if a word is a "useful english" word
            '''
            return (word is not None) and (reo.match(word) is not None)
        
        
        def process_file(f_in, f_out):
            '''
            internal helper function to keep things tidy
            
            takes data from f_in, writes to f_out
            '''
            print("file:", file_suffix)
            
            # wraps input file object with a text IO wrapper so that it can be read correctly
            f_in_txt = io.TextIOWrapper(f_in)
            
            # reads the data file into a data frame, in chunks
            chunk_counter = 0
            chunks = pd.read_csv(
                        f_in_txt, 
                        sep='\t', 
                        lineterminator='\n', 
                        header=None, 
                        names=self.file_headers,
                        chunksize=self.chunk_size,
                        quoting=csv.QUOTE_NONE)
            
            # for each frame in the read chunks ...
            frames = []
            for frame in chunks:
                chunk_counter += 1
                print("file:", file_suffix, "chunk:", chunk_counter, end="\r")
                
                # drops unnecesary columns
                frame = frame[["bi_gram", "match_count"]]
                
                # filters out any unuseful bi-grams, e.g. not two words
                filter = frame["bi_gram"].str.contains("^[A-Za-z]+ [A-Za-z]+$", na=False)
                frame = frame[filter]
                
                # if there are any row left after the filtering ...
                if frame.shape[0] > 0:
                
                    # groups by the bi-gram column to reduce the size
                    frame = frame.groupby(["bi_gram"])["match_count"].sum()
                    frame = pd.DataFrame({"match_count":frame}).reset_index()
                    
                    # splits the bi-gram into two words
                    temp = frame["bi_gram"].str.split(" ", expand=True)
                    
                    # if the bi-gram had two words in it ...
                    if temp.shape[1] == 2:
                    
                        # sets the new columns as the split words
                        frame[["bi_gram_0", "bi_gram_1"]] = temp
                    
                        # don't need the bi-gram column anymore
                        frame = frame[["bi_gram_0", "bi_gram_1", "match_count"]]
                    
                        # adds the resulting frame to the list for future use
                        frames.append(frame)
                
            # concatenates all the chunks
            big_frame = pd.concat(frames)
            
            # one more group by, to ensure no duplicates
            big_frame = big_frame.groupby(["bi_gram_0", "bi_gram_1"])["match_count"].sum()
            big_frame = pd.DataFrame({"match_count":big_frame}).reset_index()
            
            # writes to the count file
            big_frame.to_csv(
                f_out, 
                sep="\t", 
                header=False, 
                index=False, 
                encoding="utf-8", 
                line_terminator="\n", 
                quoting=csv.QUOTE_NONE)
            
            print("")            
            print("finished:", file_suffix)
                
                
                
        # opens the zipped file
        zipped_file_fullpath = self.gpfs_path + self.file_prefix + str(file_suffix) + ".csv.zip"
        with zipfile.ZipFile(zipped_file_fullpath, "r") as z:
            # unzips the file
            file_in_zip = self.file_prefix + str(file_suffix) + ".csv"
            with z.open(file_in_zip, "r") as f_in:
                # opens the output file
                file_out = self.gpfs_path + self.file_prefix + str(file_suffix) + "-count.csv"
                with open(file_out, "w") as f_out:
                    # reads from the input file and writes to output file
                    process_file(f_in, f_out)