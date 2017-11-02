import sys
import zipfile

def main():
    fullpath = sys.argv[1]
    lines_min = int(sys.argv[2])
    lines_max = int(sys.argv[3])
    
    with open(fullpath, "rb") as f:
        print("file:", fullpath)
        counter = 0
        for line in f:
            counter += 1
            if counter >= lines_min and counter < lines_max:
                print(line)
            elif counter >= lines_max:
                break

if __name__ == "__main__":
    main()