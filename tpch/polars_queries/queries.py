import argparse
import json
import os
import time
import traceback
from typing import Dict

import polars as pl
import pandas as pd
from datetime import datetime
# import ray
import sys
sys.path.append(r'C:\Users\Administrator\df-eval\tpch')
from common_utils import log_time_fn, parse_common_arguments, print_result_fn
import os
import timeit
from os.path import join

# from linetimer import CodeTimer, linetimer
from polars import testing as pl_test

dataset_dict = {}

def load_lineitem(root: str, storage_options: Dict):
    if "lineitem" not in dataset_dict:
        data_path = root + "/lineitem"
        df = pd.read_parquet(data_path, storage_options=storage_options)
        df.L_SHIPDATE = pd.to_datetime(df.L_SHIPDATE, format="%Y-%m-%d")
        df.L_RECEIPTDATE = pd.to_datetime(df.L_RECEIPTDATE, format="%Y-%m-%d")
        df.L_COMMITDATE = pd.to_datetime(df.L_COMMITDATE, format="%Y-%m-%d")
        result = pl.DataFrame(df)
        dataset_dict["lineitem"] = result
    else:
        result = dataset_dict["lineitem"]
    return result

def load_lineitem_lazy(root: str, storage_options: Dict):
    if "lineitem" not in dataset_dict:
        data_path = root + "/lineitem"

        lf = pl.read_parquet(data_path, storage_options=storage_options)

        lf = lf.with_column(pl.col("L_SHIPDATE").cast(pl.Date))
        lf = lf.with_column(pl.col("L_RECEIPTDATE").cast(pl.Date))
        lf = lf.with_column(pl.col("L_COMMITDATE").cast(pl.Date))

        # 将结果缓存到 dataset_dict 中
        dataset_dict["lineitem"] = lf

        return lf
    else:
        # 如果数据已经存在，直接返回懒加载的数据框架
        return dataset_dict["lineitem"]
    # data_path = root + "/lineitem/*.parquet"
    # p=pl.read_parquet(data_path, storage_options=storage_options)
    # exit()
    # df = pd.read_parquet(data_path, storage_options=storage_options)
    # df.L_SHIPDATE = pd.to_datetime(df.L_SHIPDATE, format="%Y-%m-%d")
    # df.L_RECEIPTDATE = pd.to_datetime(df.L_RECEIPTDATE, format="%Y-%m-%d")
    # df.L_COMMITDATE = pd.to_datetime(df.L_COMMITDATE, format="%Y-%m-%d")
    # result = df
    # dataset_dict["lineitem"] = result
    # result = pl.DataFrame(result)
    # return result

def load_part(root: str, storage_options: Dict):
    if "part" not in dataset_dict:
        # data_path = root + "/lineitem/*.parquet"
        # p=pl.read_parquet(data_path, storage_options=storage_options)
        data_path = root + "/part"
        df = pd.read_parquet(data_path, storage_options=storage_options)
        # result = df
        result=pl.DataFrame(df).lazy()
        # print(type(result))
        # result = pl.DataFrame(df)#.collect().rechunk().lazy()
        # result = p
        dataset_dict["part"] = result
    else:
        result = dataset_dict["part"]
    return result


def load_orders(root: str, storage_options: Dict):
    if "orders" not in dataset_dict:
        data_path = root + "/orders"
        df = pd.read_parquet(data_path, storage_options=storage_options)
        df.O_ORDERDATE = pd.to_datetime(df.O_ORDERDATE, format="%Y-%m-%d")
        result = df
        result=pl.from_pandas(df).lazy()
        dataset_dict["orders"] = result
    else:
        result = dataset_dict["orders"]
    return result


def load_customer(root: str, storage_options: Dict):
    if "customer" not in dataset_dict:
        data_path = root + "/customer"
        df = pd.read_parquet(data_path, storage_options=storage_options)
        # result = df
        result=pl.from_pandas(df).lazy()
        dataset_dict["customer"] = result
    else:
        result = dataset_dict["customer"]
    return result


def load_nation(root: str, storage_options: Dict):
    if "nation" not in dataset_dict:
        data_path = root + "/nation"
        df = pd.read_parquet(data_path, storage_options=storage_options)
        # result = df
        result=pl.from_pandas(df).lazy()
        dataset_dict["nation"] = result
    else:
        result = dataset_dict["nation"]
    return result


def load_region(root: str, storage_options: Dict):
    if "region" not in dataset_dict:
        data_path = root + "/region"
        df = pd.read_parquet(data_path, storage_options=storage_options)
        # result = df
        result=pl.from_pandas(df).lazy()
        dataset_dict["region"] = result
    else:
        result = dataset_dict["region"]
    return result


def load_supplier(root: str, storage_options: Dict):
    if "supplier" not in dataset_dict:
        data_path = root + "/supplier"
        df = pd.read_parquet(data_path, storage_options=storage_options)
        # result = df
        result=pl.from_pandas(df).lazy()
        dataset_dict["supplier"] = result
    else:
        result = dataset_dict["supplier"]
    return result


def load_partsupp(root: str, storage_options: Dict):
    if "partsupp" not in dataset_dict:
        data_path = root + "/partsupp"
        df = pd.read_parquet(data_path, storage_options=storage_options)
        # result = df
        result=pl.from_pandas(df).lazy()
        dataset_dict["partsupp"] = result
    else:
        result = dataset_dict["partsupp"]
    return result


def q01(root: str, storage_options: Dict):
    var_1 = pd.Timestamp("1998-09-02")
    lineitem = load_lineitem(root, storage_options)

    q_final = (
        lineitem.filter(pl.col("L_SHIPDATE") <= var_1)
        .group_by(["L_RETURNFLAG", "L_LINESTATUS"])
        .agg(
            [
                pl.sum("L_QUANTITY").alias("SUM_QTY"),
                pl.sum("L_EXTENDEDPRICE").alias("SUM_BASE_PRICE"),
                (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT")))
                .sum()
                .alias("SUM_DISC_PRICE"),
                (
                    pl.col("L_EXTENDEDPRICE")
                    * (1.0 - pl.col("L_DISCOUNT"))
                    * (1.0 + pl.col("L_TAX"))
                )
                .sum()
                .alias("SUM_CHARGE"),
                pl.mean("L_QUANTITY").alias("AVG_QTY"),
                pl.mean("L_EXTENDEDPRICE").alias("AVG_PRICE"),
                pl.mean("L_DISCOUNT").alias("AVG_DISC"),
                pl.count().alias("COUNT_ORDER"),
            ]
        )
        .sort(["L_RETURNFLAG", "L_LINESTATUS"])
    )

    return q_final

def q02(root: str, storage_options: Dict):
    var_1 = 15
    var_2 = "BRASS"
    var_3 = "EUROPE"

    part_ds = load_part(root, storage_options)
    partsupp_ds = load_partsupp(root, storage_options)
    supplier_ds = load_supplier(root, storage_options)
    nation_ds = load_nation(root, storage_options)
    region_ds = load_region(root, storage_options)

    # result_q1 = (
    #     part_ds.merge(partsupp_ds, left_on="P_PARTKEY", right_on="PS_PARTKEY")
    #     .merge(supplier_ds, left_on="PS_SUPPKEY", right_on="S_SUPPKEY")
    #     .merge(nation_ds, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
    #     .merge(region_ds, left_on="N_REGIONKEY", right_on="R_REGIONKEY")
    #     .filter(part_ds["P_SIZE"] == var_1)
    #     .filter(part_ds["P_TYPE"].str.endswith(var_2))
    #     .filter(region_ds["R_NAME"] == var_3)
    # ).cache()
    result_q1 = (
        part_ds.join(partsupp_ds, left_on="P_PARTKEY", right_on="PS_PARTKEY")
        .join(supplier_ds, left_on="PS_SUPPKEY", right_on="S_SUPPKEY")
        .join(nation_ds, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        .join(region_ds, left_on="N_REGIONKEY", right_on="R_REGIONKEY")
        .filter(pl.col("P_SIZE") == var_1)
        .filter(pl.col("P_TYPE").str.ends_with(var_2))
        .filter(pl.col("R_NAME") == var_3)
    ).cache()
    # result_q1["N_NAME"] = result_q1["N_NAME"].str.strip()  # assuming Pandas DataFrame
    # result_q1["S_NAME"] = result_q1["S_NAME"].str.strip()  # assuming Pandas DataFrame

    final_cols = [
        "S_ACCTBAL",
        "S_NAME",
        "N_NAME",
        "P_PARTKEY",
        "P_MFGR",
        "S_ADDRESS",
        "S_PHONE",
        "S_COMMENT",
    ]

    q_final = (
        result_q1.group_by("P_PARTKEY")
        .agg(pl.min("PS_SUPPLYCOST").alias("PS_SUPPLYCOST"))
        .join(
            result_q1,
            left_on=["P_PARTKEY", "PS_SUPPLYCOST"],
            right_on=["P_PARTKEY", "PS_SUPPLYCOST"],
        )
        .select(final_cols)
        .sort(
            by=["S_ACCTBAL", "N_NAME", "S_NAME", "P_PARTKEY"],
            descending=[True, False, False, False],
        )
        .limit(100)
        .with_columns(pl.col(pl.datatypes.Utf8).str.strip_chars().name.keep())
    )

    return q_final


query_to_loaders = {
    1: [load_lineitem],
    2: [load_part, load_partsupp, load_supplier, load_nation, load_region],
    # 3: [load_lineitem, load_orders, load_customer],
    # 4: [load_lineitem, load_orders],
    # 5: [
    #     load_lineitem,
    #     load_orders,
    #     load_customer,
    #     load_nation,
    #     load_region,
    #     load_supplier,
    # ],
    # 6: [load_lineitem],
    # 7: [load_lineitem, load_supplier, load_orders, load_customer, load_nation],
    # 8: [
    #     load_part,
    #     load_lineitem,
    #     load_supplier,
    #     load_orders,
    #     load_customer,
    #     load_nation,
    #     load_region,
    # ],
    # 9: [
    #     load_lineitem,
    #     load_orders,
    #     load_part,
    #     load_nation,
    #     load_partsupp,
    #     load_supplier,
    # ],
    # 10: [load_lineitem, load_orders, load_nation, load_customer],
    # 11: [load_partsupp, load_supplier, load_nation],
    # 12: [load_lineitem, load_orders],
    # 13: [load_customer, load_orders],
    # 14: [load_lineitem, load_part],
    # 15: [load_lineitem, load_supplier],
    # 16: [load_part, load_partsupp, load_supplier],
    # 17: [load_lineitem, load_part],
    # 18: [load_lineitem, load_orders, load_customer],
    # 19: [load_lineitem, load_part],
    # 20: [load_lineitem, load_part, load_nation, load_partsupp, load_supplier],
    # 21: [load_lineitem, load_orders, load_supplier, load_nation],
    # 22: [load_customer, load_orders],
}

query_to_runner = {
    1: q01,
    2: q02,
    # 3: q03,
    # 4: q04,
    # 5: q05,
    # 6: q06,
    # 7: q07,
    # 8: q08,
    # 9: q09,
    # 10: q10,
    # 11: q11,
    # 12: q12,
    # 13: q13,
    # 14: q14,
    # 15: q15,
    # 16: q16,
    # 17: q17,
    # 18: q18,
    # 19: q19,
    # 20: q20,
    # 21: q21,
    # 22: q22,
}


def run_queries(
    path,
    storage_options,
    queries,
    log_time=True,
    print_result=False,
    include_io=False,
):
    version = pl.__version__
    data_start_time = time.time()
    for query in queries:
        loaders = query_to_loaders[query]
        for loader in loaders:
            loader(path, storage_options)
    print(f"Total data loading time (s): {time.time() - data_start_time}")

    total_start = time.time()
    for query in queries:
        try:
            start_time = time.time()
            result = query_to_runner[query](path, storage_options)
            without_io_time = time.time() - start_time
            success = True
            if print_result:
                print_result_fn("pandas", result, query)
        except Exception as e:
            print("".join(traceback.TracebackException.from_exception(e).format()))
            without_io_time = 0.0
            success = False
        finally:
            pass
        if log_time:
            log_time_fn(
                "polars",
                query,
                version=version,
                without_io_time=without_io_time,
                success=success,
            )
    print(f"Total query execution time (s): {time.time() - total_start}")


def main():
    parser = argparse.ArgumentParser(description="TPC-H benchmark.")
    parser.add_argument(
        "--storage_options",
        type=str,
        required=False,
        help="storage options json file.",
    )
    parser = parse_common_arguments(parser)
    args = parser.parse_args()

    # path to TPC-H data in parquet.
    path = args.path
    print(f"Path: {path}")

    # credentials to access the datasource.
    storage_options = {}
    if args.storage_options is not None:
        with open(args.storage_options, "r") as fp:
            storage_options = json.load(fp)
    print(f"Storage options: {storage_options}")

    queries = list(range(1, 3))
    if args.queries is not None:
        queries = args.queries
    print(f"Queries to run: {queries}")
    print(f"Include IO: {args.include_io}")

    run_queries(
        path,
        storage_options,
        queries,
        args.log_time,
        args.print_result,
        args.include_io,
    )

if __name__ == "__main__":
    main()
