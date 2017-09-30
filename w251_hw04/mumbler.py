import os
import io
import sys
import subprocess
import numpy as np
import pandas as pd


def pick_next_word(word):

    # obtains the counts from each gfps node
    # TODO make parallel?
    results = ""
    for gpfs_suffix in ["gpfs1", "gpfs2", "gpfs3"]:
        remote_cmd = ["ssh", "root@"+gpfs_suffix, "grep", "-h", "-P", "'^"+word+"\t'", "/gpfs/gpfsfpo/"+gpfs_suffix+"/*-count.csv"]
        output = None
        decoded = None
        
        try:
            output = subprocess.check_output(remote_cmd)
        except subprocess.CalledProcessError:
            output = None
        
        if output is not None:
            decoded = output.decode("utf-8")
            results += decoded

    # if there were results, calculates the next word using weighted average. 
    # else, returns None as the next word
    next_word = None
    if results != "":
        # puts results into pandas for easier calculations for totals
        headers = ["gram0", "gram1", "matchcount"]
        results_str_io = io.StringIO(results)
        frame = pd.read_csv(
                    results_str_io, 
                    sep='\t', 
                    lineterminator='\n', 
                    header=None, 
                    names=headers)
        
        # calculates sub totals for every next-word
        next_word_totals_series = frame.groupby(["gram1"])["matchcount"].sum()
        
        # gets the list of candidate words and their sub totals
        candidates = next_word_totals_series.index.values
        next_word_totals = next_word_totals_series.values
        
        # calculates total for the word
        word_total = next_word_totals.sum()
        
        # calculates the weighted probabilities
        weighted_probabilites = next_word_totals / word_total
        
        # randomly chooses the next word
        next_word = np.random.choice(
                        a=candidates, 
                        p=weighted_probabilites)
                        
    else:
        next_word = None
    
    return next_word


def main():
    '''

    '''
    starting_word = sys.argv[1]
    max_num_words = int(sys.argv[2])
    
    phrase = starting_word
    current_word= starting_word
    for i in range(max_num_words-1,0,-1):
        next_word = pick_next_word(current_word)
        if next_word is None:
            break
        phrase += (" " + next_word)
        current_word = next_word 
    
    print(phrase)
    
        
if __name__ == "__main__":
    main()