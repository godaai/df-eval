# TPC-H Benchmarks
We compared Xorbits to Dask, Pandas API on Spark, and Modin on Ray with TPC-H benchmarks at scale
factor 100 (~100 GB dataset) and 1000 (~1 TB dataset). The cluster for TPC-H SF 100 consists of an
r6i.large instance as the supervisor, and 5 r6i.4xlarge instances as the workers. The cluster for
TPC-H SF 1000 consists of an r6i.large instance as the supervisor, and 16 r6i.8xlarge instances.



## Software versions
- Xorbits: 0.1.0

## SF1000: Xorbits
Although Xorbits is able to pass all the queries in a row, Dask, Pandas API on Spark and Modin 
failed on most of the queries. Thus, we are not able to compare the performance difference now, and
we plan to try again later.

![image info](https://xorbits.io/res/xorbits_1t.png)

|     |  Xorbits  |
|:---:|:---------:|
| Q01 |  141.810  |
| Q02 |   35.000  |
| Q03 |  194.630  |
| Q04 |  225.570  |
| Q05 |  185.560  |
| Q06 |  101.430  |
| Q07 |  157.150  |
| Q08 |  143.060  |
| Q09 |  249.270  |
| Q10 |  131.940  |
| Q11 |   31.823  |
| Q12 |   89.139  |
| Q13 |   40.157  |
| Q14 |   76.638  |
| Q15 |  108.130  |
| Q16 |   43.952  |
| Q17 |  304.620  |
| Q18 |  126.880  |
| Q19 |   69.102  |
| Q20 |  103.000  |
| Q21 |  454.020  |
| Q22 |  162.460  |
|Total|  3012.88  |

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