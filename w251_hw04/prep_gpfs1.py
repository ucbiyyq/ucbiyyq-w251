from DataProcessor import DataProcessor
  
def process_node_summary():
    '''
    Computes the summary counts for all the files on this gpfs node.
    
    Summary file is output as a pickled Panda dataframe, 
    to the gpfs node's counts folder.
    '''
    
    # creates an instance of the data processing helper class
    dp = DataProcessor(gpfs_suffix = "gpfs1")
    
    ## cleans out the local temp pickles
    dp.delete_local_pickles()
    
    # for the files in the range of suffixes on this gpfs server, converts the chunks of files into dataframes
    for i in range(0, 32):
        dp.prep_file(file_suffix=i, nrows=None, chunk_size=1000000)
        
    # concatenates all the dataframes generated lcoally from the data files, and pickles them to the given destination
    dp.concat_local_pickles()

    
process_node_summary()