import zipfile

file_suffix = 0
gpfs_suffix = "gpfs1"
gpfs_prefix = "/gpfs/gpfsfpo/"
file_prefix = "googlebooks-eng-all-2gram-20090715-"


def peek_file(lines_min, lines_max):
    with zipfile.ZipFile(gpfs_prefix + gpfs_suffix + "/" + file_prefix + str(file_suffix) + ".csv.zip", "r") as z:
        with z.open(file_prefix + str(file_suffix) + ".csv", "r") as f:
            print("file:", file_suffix)
            counter = 0
            for line in f:
                counter += 1
                if counter >= lines_min and counter < lines_max:
                    print(line)
                elif counter >= lines_max:
                    break

peek_file(767350, 767359)