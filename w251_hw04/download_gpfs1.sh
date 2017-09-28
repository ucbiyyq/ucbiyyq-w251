url_prefix=http://storage.googleapis.com/books/ngrams/books/
file_prefix=googlebooks-eng-all-2gram-20090715-
zip_suffix=.csv.zip
gpfs_path=/gpfs/gpfsfpo/gpfs1/


pushd ${gpfs_path}
curl ${url_prefix}${file_prefix}[0-32]${zip_suffix} -o ${file_prefix}#1${zip_suffix}
unzip -o "*.zip"
popd
