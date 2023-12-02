# TPC-H Benchmarks


## Run

### Environment Setup

Each folder contains a `deploy.md` file which descripes how to set up the environment via Docker container. For distributed engines like Dask, Ray, Spark, and Xorbits, we need an cluster endpoint that the client can connect with.

### Run the queries

Note that we should run the queries **under** the `tpch` folder while using the `-m` flags to launch a specific query. 

Here are some arguments that we can specify when launching the query scirpt:

* `--path`: the path of the TPC-H datasets. If the `path` is on S3, we should also specify `storage_options` that contains AWS accounts and keys.

* `--queries`: the queries that we want to run. Query number should be seperated by whitespace, for example: `--queries 1 2`. If `queries` is not specified, all the 22 queries will be executed.

* `--log_time`: log the execution time.

* `--print_result`: print the query result and save into files.

For example, lanching the pandas script should be like:

```
python -u -m pandas_queries.queries \
    --path /path/to/tpch/SF10 \
    --log_time \
    --print_result \
    --queries 1 \
```