from DataProcessor import DataProcessor


  
def process_local_data():
    '''
    
    '''
    
    # creates an instance of the data processing helper class
    dp = DataProcessor(gpfs_suffix = "gpfs1")
    
    ## cleans out the local temp pickles
    #dp.delete_local_pickles()
    
    ## for the files in the range of suffixes on this gpfs server, converts the chunks of files into dataframes
    #for i in range(20, 22):
    #    dp.prep_file(file_suffix=i, nrows=300000, chunk_size=100000)
        
    # concatenates all the dataframes generated lcoally from the data files, and pickles them to the given destination
    dp.concat_local_pickles()
    
    #frames = []
    #temp_df = pd.concat(frames)
    ## groups by the bi-gram-0, bi-gram-1, and match_count columns, to save some space
    #temp_df = temp_df.groupby(["bi_gram_0", "bi_gram_1"])["match_count"].sum()
    #temp_df = pd.DataFrame({"match_count":temp_df}).reset_index()
    #result = temp_df
    #print(result)
    ## pickles the resulting dataframe
    #result.to_pickle(local_temp_folder_prefix + "gpfs1.pkl")
    

    
process_local_data()