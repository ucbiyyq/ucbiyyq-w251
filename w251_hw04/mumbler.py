import os
import io
import sys
import subprocess
import numpy as np
import pandas as pd

# used to store dataframes of words that have been found before in this run
cache = {}

def pick_next_word(word):
    '''
    Given a word, picks the next word based on the weighted averages of all following words
    '''
    
    def remote_check(word):
        '''
        internal helper function that checks each of gfps nodes for the word
        given a word, returns a list of candiate next words, and their weight probabilities
        if the word doesn't exist, returns None, None
        '''
        # obtains the counts from each gfps node
        # in parallel, kicks off all the gpfs node's grep processes
        processes = []
        for gpfs_suffix in ["gpfs1", "gpfs2", "gpfs3"]:
            remote_cmd = ["ssh", "root@"+gpfs_suffix, "grep", "-h", "-P", "'^"+word+"\t'", "/gpfs/gpfsfpo/"+gpfs_suffix+"/*-count.csv"]
            proc = subprocess.Popen(remote_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processes.append(proc)
            
        # waits for the remote grep to return with the answers
        outs = []
        for proc in processes:
            output = None
            try:
                output, errs = proc.communicate(timeout=30)
            except TimeoutExpired:
                proc.kill()
                output, errs = proc.communicate()
            outs.append(output)
            
        # converts any results to a string, so that we can later calculate the totals
        outs_b = ""
        for output in outs:
            decoded = output.decode("utf-8")
            outs_b += decoded

        # puts results into pandas for easier calculations for totals
        frame = None
        if outs_b != "":
            headers = ["gram0", "gram1", "matchcount"]
            outs_str_io = io.StringIO(outs_b)
            frame = pd.read_csv(
                        outs_str_io, 
                        sep='\t', 
                        lineterminator='\n', 
                        header=None, 
                        names=headers)
                        
        # if there is a result, calculates the candidates and weighted possibilities
        candidates = None
        weighted_probabilites = None
        if frame is not None:
        
            # calculates sub totals for every next-word
            next_word_totals_series = frame.groupby(["gram1"])["matchcount"].sum()
            
            # gets the list of candidate words and their sub totals
            candidates = next_word_totals_series.index.values
            next_word_totals = next_word_totals_series.values
            
            # calculates total for the word
            word_total = next_word_totals.sum()
            
            # calculates the weighted probabilities
            weighted_probabilites = next_word_totals / word_total
        
        # returns the results of the remote check
        return candidates, weighted_probabilites


    
    # checks cache to see if we've found this word before
    # if new, asks the gpfs nodes for the answer
    candidates = None
    weighted_probabilites = None
    if word not in cache:
        candidates, weighted_probabilites = remote_check(word)
        cache[word] = candidates, weighted_probabilites
    else:
        candidates, weighted_probabilites = cache[word]
    
    # if there were results, calculates the next word using weighted average. 
    # else, returns None as the next word
    next_word = None
    if candidates is not None:
        # randomly chooses the next word
        next_word = np.random.choice(
                        a=candidates, 
                        p=weighted_probabilites)
    else:
        next_word = None
    
    # returns the chosen next word
    return next_word


def main():
    '''
    Mumbler
    
    Parameters
        1. starting word
        2. max number of words
        3. optional, random seed
    
    Example Usage
        $ python mumbler.py smithy 10
        $ python mumbler.py smithy 10 42
    '''
    starting_word = sys.argv[1]
    max_num_words = int(sys.argv[2])
    random_seed = None
    if len(sys.argv) >= 4:
        random_seed = int(sys.argv[3])
    
    # sets random seed if that was entered
    if random_seed is not None:
        np.random.seed(random_seed)
    
    # sets the starting word, 
    # then generates a chain of words as long as the given max number of words
    phrase = starting_word
    current_word= starting_word
    for i in range(max_num_words-1,0,-1):
        next_word = pick_next_word(current_word)
        # if word chain ends before max number of words reached, breaks
        if next_word is None:
            break
        phrase += (" " + next_word)
        current_word = next_word 
    
    # outputs the phrase
    print(phrase)
    
        
if __name__ == "__main__":
    main()