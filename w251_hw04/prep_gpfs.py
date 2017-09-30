import sys
from DataProcessor import DataProcessor

def main():
    '''
    Computes the summary counts for all the files on this gpfs node.
    
    Each summary count file is output to the same gfps folder as the input file
    '''
    gpfs_suffix = sys.argv[1]
    file_min = int(sys.argv[2])
    file_max = int(sys.argv[3])
    nrows = None
    if len(sys.argv) >= 5:
        nrows = int(sys.argv[4])
    
    # creates an instance of the data processing helper class
    dp = DataProcessor(gpfs_suffix = gpfs_suffix)
    
    # for the files in the range of suffixes on this gpfs server, converts the chunks of files into dataframes
    for i in range(file_min, file_max+1):
        dp.prep_file(file_suffix=i, nrows=nrows)

        
        
if __name__ == "__main__":
    main()