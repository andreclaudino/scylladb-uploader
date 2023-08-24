# ScyllaDB-uploader

A system to upload JSON or CSV files to MongoDB from local file os s3-like object storage.

## Usage


```
$> scylladb-uploader --help

Usage: scylladb-uploader [OPTIONS] --source-path <SOURCE_PATH> --connection-string <CONNECTION_STRING> --database-name <DATABASE_NAME> --database-collection <DATABASE_COLLECTION> --batch-size <BATCH_SIZE> --s3-access-key <S3_ACCESS_KEY> --s3-secret-access-key <S3_SECRET_ACCESS_KEY>

Options:
  -s, --source-path <SOURCE_PATH>
          Source path [env: SOURCE_PATH=]
      --source-file-type <SOURCE_FILE_TYPE>
          Source file type [env: SOURCE_FILE_PATH=] [default: json] [possible values: json, csv]
      --database-nodes <DATABASE_HOSTS>
          Comma separated database nodes (host:port) list [env: DATABASE_NODES=]
      --database-name <DATABASE_NAME>
          Database name [env: DATABASE_NAME=]
      --database-collection <DATABASE_COLLECTION>
          Database collection [env: DATABASE_COLLECTION=]
      --batch-size <BATCH_SIZE>
          Database collection [env: BATCH_SIZE=]
      --s3-endpoint <S3_ENDPOINT>
          The S3 endpoint to connect and save file [env: S3_ENDPOINT=] [default: http://minio.storage.svc]
      --s3-region <S3_REGION>
          S3 Region to connect (blank for minio) [env: S3_NEW_PATH_STYLE=] [default: ]
      --s3-access-key <S3_ACCESS_KEY>
          S3 Access key [env: S3_ACCESS_KEY=]
      --s3-secret-access-key <S3_SECRET_ACCESS_KEY>
          S3 Secret Access key [env: S3_SECRET_ACCESS_KEY=]
  -h, --help
          Print help
  -V, --version
          Print version
```

## Docker image

A docker image is available at `docker.io/andreclaudino/scylladb-uploader`.

Current available tags are:

* latest
* 0.1.0
