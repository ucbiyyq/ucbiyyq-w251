import sys
import zipfile

def main():
    gpfs_fullpath = sys.argv[1]
    lines_min = int(sys.argv[2])
    lines_max = int(sys.argv[3])
    
    with zipfile.ZipFile(gpfs_fullpath, "r") as z:
        for file in z.namelist():
            with z.open(file, "r") as f:
                print("zip:", gpfs_fullpath, "file:", file)
                counter = 0
                for line in f:
                    counter += 1
                    if counter >= lines_min and counter < lines_max:
                        print(line)
                    elif counter >= lines_max:
                        break

if __name__ == "__main__":
    main()