# PGS Catalog Metadata Exports
Package to export PGS Catalog metadata

Usage
```
usage: pgs_metadata_exports.py [-h] --url URL --dir DIR [--remote_ftp]

optional arguments:
  -h, --help    show this help message and exit
  --url URL     The URL root of the REST API, e.g. "http://127.0.0.1:8000/rest/"
  --dir DIR     The path of the root dir of the metadata "<dir>/new_ftp_content"
  --remote_ftp  Flag to indicate whether the FTP is remote (FTP protocol) or local (file system)
```