import io
import os
import sys
import re
import zipfile

class DataProcessor(object):
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
            
            # regex pattern to be used to determine usefulness of ngram
            # useful is defined as having all characters a to z, or A to Z
            pattern = "^[A-Za-z]+$"
            reo = re.compile(pattern)
            
            # wraps input file object with a text IO wrapper so that it can be read correctly
            f_in_txt = io.TextIOWrapper(f_in)
            
            # sets the initial variables
            line_counter = 0
            prev_ngram_0 = None
            prev_ngram_1 = None
            cumulative_match_count = 0
            
            # for each line in the file ...
            for line in f_in_txt:
                line_counter += 1
                sys.stdout.write("\rfile: %s line: %i" % (file_suffix, line_counter))
                sys.stdout.flush()
                
                # gets the ngram and match count tokens
                tokens = line.split("\t")
                ngram = str(tokens[0])
                match_count = int(tokens[2])
                
                # gets each of the words in the ngram
                ngram = ngram
                ngram_tokens = ngram.split(" ")
                ngram_tokens_len = len(ngram_tokens)
                ngram_0 = None
                ngram_1 = None
                if ngram_tokens_len >= 1:
                    ngram_0 = ngram_tokens[0]
                    ngram_0 = str(ngram_0)
                if ngram_tokens_len >= 2:
                    ngram_1 = ngram_tokens[1]
                    ngram_1 = str(ngram_1)
                
                # determines if the match count of current ngram needs to be added to the previous line's match count
                if (prev_ngram_0 is None) and (prev_ngram_1 is None):
                    # if at start, or haven't found a useful ngram, sets the initial values
                    prev_ngram_0 = ngram_0
                    prev_ngram_1 = ngram_1
                    cumulative_match_count = match_count
                elif (ngram_0 == prev_ngram_0) and (ngram_1 == prev_ngram_1):
                    # if ngram is the same as the previous line, adds the match count value
                    cumulative_match_count += match_count
                else:
                    # otherwise, outputs the previous ngram & accumulated match count, and resets the ngram & match count
                    # if the previous ngram is useful, writes to output
                    keep1 = is_useful(reo, prev_ngram_0)
                    keep2 = is_useful(reo, prev_ngram_1)
                    if (keep1 and keep2):
                        print(prev_ngram_0 + "\t" + prev_ngram_1 + "\t" + str(cumulative_match_count), file=f_out)
                    prev_ngram_0 = ngram_0
                    prev_ngram_1 = ngram_1
                    cumulative_match_count = match_count
                
                # checks if we're over the nrows limit
                if (nrows is not None) and (line_counter >= nrows):
                    break
            
            # at end of the file, outputs the last ngram and accumulated match count, if useful
            keep1 = is_useful(reo, prev_ngram_0)
            keep2 = is_useful(reo, prev_ngram_1)
            if (keep1 and keep2):
                print(prev_ngram_0 + "\t" + prev_ngram_1 + "\t" + str(cumulative_match_count), file=f_out)
            
            # finished processing the file
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