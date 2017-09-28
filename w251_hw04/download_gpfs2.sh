url_prefix=http://storage.googleapis.com/books/ngrams/books/
temp_prefix=/root/temp/
file_prefix=googlebooks-eng-all-2gram-20090715-
zip_suffix=.csv.zip
file_suffix=.csv
gpfs_prefix=/gpfs/gpfsfpo/gpfs2/

for i in {33..35}; do
    url=${url_prefix}${file_prefix}${i}${zip_suffix}
    file=${temp_prefix}${file_prefix}${i}${zip_suffix}
    wget $url -O $file
    unzip -o $file -d $gpfs_prefix
done
