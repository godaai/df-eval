import argparse
import json
import os
import time
import traceback
from datetime import datetime
from typing import Dict

import polars as pl

from common_utils import log_time_fn, parse_common_arguments, print_result_fn

dataset_dict = {}


def load_lineitem_lazy(root: str, storage_options: Dict):
    if "lineitem" not in dataset_dict:
        data_path = root + "/lineitem/*.parquet"
        p = pl.read_parquet(data_path, storage_options=storage_options).lazy()
        p = p.with_columns(pl.col("L_SHIPDATE").cast(pl.Date))
        p = p.with_columns(pl.col("L_RECEIPTDATE").cast(pl.Date))
        p = p.with_columns(pl.col("L_COMMITDATE").cast(pl.Date))
        result = p
        dataset_dict["lineitem"] = result
    else:
        result = dataset_dict["lineitem"]
    return result


def load_part_lazy(root: str, storage_options: Dict):
    if "part" not in dataset_dict:
        data_path = root + "/part/*.parquet"
        result = pl.read_parquet(data_path, storage_options=storage_options).lazy()
        dataset_dict["part"] = result
    else:
        result = dataset_dict["part"]
    return result


def load_orders_lazy(root: str, storage_options: Dict):
    if "orders" not in dataset_dict:
        data_path = root + "/orders/*.parquet"
        result = pl.read_parquet(data_path, storage_options=storage_options).lazy()
        result = result.with_columns(pl.col("O_ORDERDATE").cast(pl.Date))
        dataset_dict["orders"] = result
    else:
        result = dataset_dict["orders"]
    return result


def load_customer_lazy(root: str, storage_options: Dict):
    if "customer" not in dataset_dict:
        data_path = root + "/customer/*.parquet"
        result = pl.read_parquet(data_path, storage_options=storage_options).lazy()
        dataset_dict["customer"] = result
    else:
        result = dataset_dict["customer"]
    return result


def load_nation_lazy(root: str, storage_options: Dict):
    if "nation" not in dataset_dict:
        data_path = root + "/nation/*.parquet"
        result = pl.read_parquet(data_path, storage_options=storage_options).lazy()
        dataset_dict["nation"] = result
    else:
        result = dataset_dict["nation"]
    return result


def load_region_lazy(root: str, storage_options: Dict):
    if "region" not in dataset_dict:
        data_path = root + "/region/*.parquet"
        result = pl.read_parquet(data_path, storage_options=storage_options).lazy()
        dataset_dict["region"] = result
    else:
        result = dataset_dict["region"]
    return result


def load_supplier_lazy(root: str, storage_options: Dict):
    if "supplier" not in dataset_dict:
        data_path = root + "/supplier/*.parquet"
        result = pl.read_parquet(data_path, storage_options=storage_options).lazy()
        dataset_dict["supplier"] = result
    else:
        result = dataset_dict["supplier"]
    return result


def load_partsupp_lazy(root: str, storage_options: Dict):
    if "partsupp" not in dataset_dict:
        data_path = root + "/partsupp/*.parquet"
        result = pl.read_parquet(data_path, storage_options=storage_options).lazy()
        dataset_dict["partsupp"] = result
    else:
        result = dataset_dict["partsupp"]
    return result


def q01(root: str, storage_options: Dict):
    var_1 = datetime(1998, 9, 2)
    lineitem = load_lineitem_lazy(root, storage_options)
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

    part_ds = load_part_lazy(root, storage_options)
    partsupp_ds = load_partsupp_lazy(root, storage_options)
    supplier_ds = load_supplier_lazy(root, storage_options)
    nation_ds = load_nation_lazy(root, storage_options)
    region_ds = load_region_lazy(root, storage_options)

    result_q1 = (
        part_ds.join(partsupp_ds, left_on="P_PARTKEY", right_on="PS_PARTKEY")
        .join(supplier_ds, left_on="PS_SUPPKEY", right_on="S_SUPPKEY")
        .join(nation_ds, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        .join(region_ds, left_on="N_REGIONKEY", right_on="R_REGIONKEY")
        .filter(pl.col("P_SIZE") == var_1)
        .filter(pl.col("P_TYPE").str.ends_with(var_2))
        .filter(pl.col("R_NAME") == var_3)
    ).cache()

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


def q03(root: str, storage_options: Dict):
    var_1 = var_2 = datetime(1995, 3, 15)
    var_3 = "BUILDING"

    customer_ds = load_customer_lazy(root, storage_options)
    line_item_ds = load_lineitem_lazy(root, storage_options)
    orders_ds = load_orders_lazy(root, storage_options)
    q_final = (
        customer_ds.filter(pl.col("C_MKTSEGMENT") == var_3)
        .join(orders_ds, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        .join(line_item_ds, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .filter(pl.col("O_ORDERDATE") < var_2)
        .filter(pl.col("L_SHIPDATE") > var_1)
        .with_columns(
            (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).alias("REVENUE")
        )
        .group_by(["O_ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY"])
        .agg([pl.sum("REVENUE")])
        .select(
            [
                pl.col("O_ORDERKEY").alias("L_ORDERKEY"),
                "REVENUE",
                "O_ORDERDATE",
                "O_SHIPPRIORITY",
            ]
        )
        .sort(by=["REVENUE", "O_ORDERDATE"], descending=[True, False])
        .limit(10)
    )

    return q_final


def q04(root: str, storage_options: Dict):
    var_1 = datetime(1993, 7, 1)
    var_2 = datetime(1993, 10, 1)

    line_item_ds = load_lineitem_lazy(root, storage_options)
    orders_ds = load_orders_lazy(root, storage_options)

    q_final = (
        line_item_ds.join(orders_ds, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
        .filter(pl.col("O_ORDERDATE").is_between(var_1, var_2, closed="left"))
        .filter(pl.col("L_COMMITDATE") < pl.col("L_RECEIPTDATE"))
        .unique(subset=["O_ORDERPRIORITY", "L_ORDERKEY"])
        .group_by("O_ORDERPRIORITY")
        .agg(pl.count().alias("ORDER_COUNT"))
        .sort(by="O_ORDERPRIORITY")
        .with_columns(pl.col("ORDER_COUNT").cast(pl.datatypes.Int64))
    )

    return q_final


def q05(root: str, storage_options: Dict):
    var_1 = "ASIA"
    var_2 = datetime(1994, 1, 1)
    var_3 = datetime(1995, 1, 1)

    region_ds = load_region_lazy(root, storage_options)
    nation_ds = load_nation_lazy(root, storage_options)
    customer_ds = load_customer_lazy(root, storage_options)
    line_item_ds = load_lineitem_lazy(root, storage_options)
    orders_ds = load_orders_lazy(root, storage_options)
    supplier_ds = load_supplier_lazy(root, storage_options)

    q_final = (
        region_ds.join(nation_ds, left_on="R_REGIONKEY", right_on="N_REGIONKEY")
        .join(customer_ds, left_on="N_NATIONKEY", right_on="C_NATIONKEY")
        .join(orders_ds, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        .join(line_item_ds, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .join(
            supplier_ds,
            left_on=["L_SUPPKEY", "N_NATIONKEY"],
            right_on=["S_SUPPKEY", "S_NATIONKEY"],
        )
        .filter(pl.col("R_NAME") == var_1)
        .filter(pl.col("O_ORDERDATE").is_between(var_2, var_3, closed="left"))
        .with_columns(
            (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).alias("REVENUE")
        )
        .group_by("N_NAME")
        .agg([pl.sum("REVENUE")])
        .sort(by="REVENUE", descending=True)
    )

    return q_final


def q06(root: str, storage_options: Dict):
    var_1 = datetime(1994, 1, 1)
    var_2 = datetime(1995, 1, 1)
    var_3 = 24

    line_item_ds = load_lineitem_lazy(root, storage_options)

    q_final = (
        line_item_ds.filter(
            pl.col("L_SHIPDATE").is_between(var_1, var_2, closed="left")
        )
        .filter(pl.col("L_DISCOUNT").is_between(0.05, 0.07))
        .filter(pl.col("L_QUANTITY") < var_3)
        .with_columns(
            (pl.col("L_EXTENDEDPRICE") * pl.col("L_DISCOUNT")).alias("REVENUE")
        )
        .select(pl.sum("REVENUE").alias("REVENUE"))
    )
    return q_final


def q07(root: str, storage_options: Dict):
    nation_ds = load_nation_lazy(root, storage_options)
    customer_ds = load_customer_lazy(root, storage_options)
    line_item_ds = load_lineitem_lazy(root, storage_options)
    orders_ds = load_orders_lazy(root, storage_options)
    supplier_ds = load_supplier_lazy(root, storage_options)

    n1 = nation_ds.filter(pl.col("N_NAME") == "FRANCE")
    n2 = nation_ds.filter(pl.col("N_NAME") == "GERMANY")

    var_1 = datetime(1995, 1, 1)
    var_2 = datetime(1996, 12, 31)

    df1 = (
        customer_ds.join(n1, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
        .join(orders_ds, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        .rename({"N_NAME": "CUST_NATION"})
        .join(line_item_ds, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .join(supplier_ds, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
        .join(n2, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        .rename({"N_NAME": "SUPP_NATION"})
    )

    df2 = (
        customer_ds.join(n2, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
        .join(orders_ds, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        .rename({"N_NAME": "CUST_NATION"})
        .join(line_item_ds, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .join(supplier_ds, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
        .join(n1, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        .rename({"N_NAME": "SUPP_NATION"})
    )

    q_final = (
        pl.concat([df1, df2])
        .filter(pl.col("L_SHIPDATE").is_between(var_1, var_2))
        .with_columns(
            (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).alias("VOLUME")
        )
        .with_columns(pl.col("L_SHIPDATE").dt.year().alias("L_YEAR"))
        .group_by(["SUPP_NATION", "CUST_NATION", "L_YEAR"])
        .agg([pl.sum("VOLUME").alias("REVENUE")])
        .sort(by=["SUPP_NATION", "CUST_NATION", "L_YEAR"])
    )
    return q_final


def q08(root: str, storage_options: Dict):
    part_ds = load_part_lazy(root, storage_options)
    supplier_ds = load_supplier_lazy(root, storage_options)
    line_item_ds = load_lineitem_lazy(root, storage_options)
    orders_ds = load_orders_lazy(root, storage_options)
    customer_ds = load_customer_lazy(root, storage_options)
    nation_ds = load_nation_lazy(root, storage_options)
    region_ds = load_region_lazy(root, storage_options)

    n1 = nation_ds.select(["N_NATIONKEY", "N_REGIONKEY"])
    n2 = nation_ds.clone().select(["N_NATIONKEY", "N_NAME"])

    q_final = (
        part_ds.join(line_item_ds, left_on="P_PARTKEY", right_on="L_PARTKEY")
        .join(supplier_ds, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
        .join(orders_ds, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
        .join(customer_ds, left_on="O_CUSTKEY", right_on="C_CUSTKEY")
        .join(n1, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
        .join(region_ds, left_on="N_REGIONKEY", right_on="R_REGIONKEY")
        .filter(pl.col("R_NAME") == "AMERICA")
        .join(n2, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        .filter(
            pl.col("O_ORDERDATE").is_between(
                datetime(1995, 1, 1), datetime(1996, 12, 31)
            )
        )
        .filter(pl.col("P_TYPE") == "ECONOMY ANODIZED STEEL")
        .select(
            [
                pl.col("O_ORDERDATE").dt.year().alias("O_YEAR"),
                (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).alias(
                    "VOLUME"
                ),
                pl.col("N_NAME").alias("NATION"),
            ]
        )
        .with_columns(
            pl.when(pl.col("NATION") == "BRAZIL")
            .then(pl.col("VOLUME"))
            .otherwise(0)
            .alias("_tmp")
        )
        .group_by("O_YEAR")
        .agg((pl.sum("_tmp") / pl.sum("VOLUME")).round(2).alias("MKT_SHARE"))
        .sort("O_YEAR")
    )
    return q_final


def q09(root: str, storage_options: Dict):
    part_ds = load_part_lazy(root, storage_options)
    supplier_ds = load_supplier_lazy(root, storage_options)
    line_item_ds = load_lineitem_lazy(root, storage_options)
    part_supp_ds = load_partsupp_lazy(root, storage_options)
    orders_ds = load_orders_lazy(root, storage_options)
    nation_ds = load_nation_lazy(root, storage_options)

    q_final = (
        line_item_ds.join(supplier_ds, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
        .join(
            part_supp_ds,
            left_on=["L_SUPPKEY", "L_PARTKEY"],
            right_on=["PS_SUPPKEY", "PS_PARTKEY"],
        )
        .join(part_ds, left_on="L_PARTKEY", right_on="P_PARTKEY")
        .join(orders_ds, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
        .join(nation_ds, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        .filter(pl.col("P_NAME").str.contains("green"))
        .select(
            [
                pl.col("N_NAME").alias("NATION"),
                pl.col("O_ORDERDATE").dt.year().alias("O_YEAR"),
                (
                    pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))
                    - pl.col("PS_SUPPLYCOST") * pl.col("L_QUANTITY")
                ).alias("AMOUNT"),
            ]
        )
        .group_by(["NATION", "O_YEAR"])
        .agg(pl.sum("AMOUNT").round(2).alias("SUM_PROFIT"))
        .sort(by=["NATION", "O_YEAR"], descending=[False, True])
    )
    return q_final


def q10(root: str, storage_options: Dict):
    customer_ds = load_customer_lazy(root, storage_options)
    orders_ds = load_orders_lazy(root, storage_options)
    line_item_ds = load_lineitem_lazy(root, storage_options)
    nation_ds = load_nation_lazy(root, storage_options)

    var_1 = datetime(1993, 10, 1)
    var_2 = datetime(1994, 1, 1)

    q_final = (
        customer_ds.join(orders_ds, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        .join(line_item_ds, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .join(nation_ds, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
        .filter(pl.col("O_ORDERDATE").is_between(var_1, var_2, closed="left"))
        .filter(pl.col("L_RETURNFLAG") == "R")
        .group_by(
            [
                "C_CUSTKEY",
                "C_NAME",
                "C_ACCTBAL",
                "C_PHONE",
                "N_NAME",
                "C_ADDRESS",
                "C_COMMENT",
            ]
        )
        .agg(
            [
                (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT")))
                .sum()
                .round(2)
                .alias("REVENUE")
            ]
        )
        .with_columns(
            pl.col("C_ADDRESS").str.strip_chars(),
            pl.col("C_COMMENT").str.strip_chars(),
        )
        .select(
            [
                "C_CUSTKEY",
                "C_NAME",
                "REVENUE",
                "C_ACCTBAL",
                "N_NAME",
                "C_ADDRESS",
                "C_PHONE",
                "C_COMMENT",
            ]
        )
        .sort(by="REVENUE", descending=True)
        .limit(20)
    )
    return q_final


def q11(root: str, storage_options: Dict):
    supplier_ds = load_supplier_lazy(root, storage_options)
    part_supp_ds = load_partsupp_lazy(root, storage_options)
    nation_ds = load_nation_lazy(root, storage_options)

    var_1 = "GERMANY"
    var_2 = 0.0001

    res_1 = (
        part_supp_ds.join(supplier_ds, left_on="PS_SUPPKEY", right_on="S_SUPPKEY")
        .join(nation_ds, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        .filter(pl.col("N_NAME") == var_1)
    )
    res_2 = res_1.select(
        (pl.col("PS_SUPPLYCOST") * pl.col("PS_AVAILQTY")).sum().round(2).alias("TMP")
        * var_2
    ).with_columns(pl.lit(1).alias("LIT"))

    q_final = (
        res_1.group_by("PS_PARTKEY")
        .agg(
            (pl.col("PS_SUPPLYCOST") * pl.col("PS_AVAILQTY"))
            .sum()
            .round(2)
            .alias("VALUE")
        )
        .with_columns(pl.lit(1).alias("LIT"))
        .join(res_2, on="LIT")
        .filter(pl.col("VALUE") > pl.col("TMP"))
        .select(["PS_PARTKEY", "VALUE"])
        .sort("VALUE", descending=True)
    )
    return q_final


def q12(root: str, storage_options: Dict):
    line_item_ds = load_lineitem_lazy(root, storage_options)
    orders_ds = load_orders_lazy(root, storage_options)

    var_1 = "MAIL"
    var_2 = "SHIP"
    var_3 = datetime(1994, 1, 1)
    var_4 = datetime(1995, 1, 1)

    q_final = (
        orders_ds.join(line_item_ds, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .filter(pl.col("L_SHIPMODE").is_in([var_1, var_2]))
        .filter(pl.col("L_COMMITDATE") < pl.col("L_RECEIPTDATE"))
        .filter(pl.col("L_SHIPDATE") < pl.col("L_COMMITDATE"))
        .filter(pl.col("L_RECEIPTDATE").is_between(var_3, var_4, closed="left"))
        .with_columns(
            [
                pl.when(pl.col("O_ORDERPRIORITY").is_in(["1-URGENT", "2-HIGH"]))
                .then(1)
                .otherwise(0)
                .alias("HIGH_LINE_COUNT"),
                pl.when(pl.col("O_ORDERPRIORITY").is_in(["1-URGENT", "2-HIGH"]).not_())
                .then(1)
                .otherwise(0)
                .alias("LOW_LINE_COUNT"),
            ]
        )
        .group_by("L_SHIPMODE")
        .agg([pl.col("HIGH_LINE_COUNT").sum(), pl.col("LOW_LINE_COUNT").sum()])
        .sort("L_SHIPMODE")
    )
    return q_final


def q13(root: str, storage_options: Dict):
    var_1 = "special"
    var_2 = "requests"

    customer_ds = load_customer_lazy(root, storage_options)
    orders_ds = load_orders_lazy(root, storage_options).filter(
        pl.col("O_COMMENT").str.contains(f"{var_1}.*{var_2}").not_()
    )
    q_final = (
        customer_ds.join(
            orders_ds, left_on="C_CUSTKEY", right_on="O_CUSTKEY", how="left"
        )
        .group_by("C_CUSTKEY")
        .agg(
            [
                pl.col("O_ORDERKEY").count().alias("C_COUNT"),
                pl.col("O_ORDERKEY").null_count().alias("NULL_C_COUNT"),
            ]
        )
        .with_columns((pl.col("C_COUNT") - pl.col("NULL_C_COUNT")).alias("C_COUNT"))
        .group_by("C_COUNT")
        .count()
        .select([pl.col("C_COUNT"), pl.col("COUNT").alias("CUSTDIST")])
        .sort(["CUSTDIST", "C_COUNT"], descending=[True, True])
    )
    return q_final


def q14(root: str, storage_options: Dict):
    line_item_ds = load_lineitem_lazy(root, storage_options)
    part_ds = load_part_lazy(root, storage_options)

    var_1 = datetime(1995, 9, 1)
    var_2 = datetime(1995, 10, 1)

    q_final = (
        line_item_ds.join(part_ds, left_on="L_PARTKEY", right_on="P_PARTKEY")
        .filter(pl.col("L_SHIPDATE").is_between(var_1, var_2, closed="left"))
        .select(
            (
                100.00
                * pl.when(pl.col("P_TYPE").str.contains("PROMO*"))
                .then((pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))))
                .otherwise(0)
                .sum()
                / (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).sum()
            )
            .round(2)
            .alias("PROMO_REVENUE")
        )
    )
    return q_final


def q15(root: str, storage_options: Dict):
    line_item_ds = load_lineitem_lazy(root, storage_options)
    supplier_ds = load_supplier_lazy(root, storage_options)

    var_1 = datetime(1996, 1, 1)
    var_2 = datetime(1996, 4, 1)

    revenue_ds = (
        line_item_ds.filter(
            pl.col("L_SHIPDATE").is_between(var_1, var_2, closed="left")
        )
        .group_by("L_SUPPKEY")
        .agg(
            (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT")))
            .sum()
            .alias("TOTAL_REVENUE")
        )
        .select([pl.col("L_SUPPKEY").alias("SUPPLIER_NO"), pl.col("TOTAL_REVENUE")])
    )

    q_final = (
        supplier_ds.join(revenue_ds, left_on="S_SUPPKEY", right_on="SUPPLIER_NO")
        .filter(pl.col("TOTAL_REVENUE") == pl.col("TOTAL_REVENUE").max())
        .with_columns(pl.col("TOTAL_REVENUE").round(2))
        .select(["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE", "TOTAL_REVENUE"])
        .sort("S_SUPPKEY")
    )
    return q_final


def q16(root: str, storage_options: Dict):
    part_supp_ds = load_partsupp_lazy(root, storage_options)
    part_ds = load_part_lazy(root, storage_options)
    supplier_ds = (
        load_supplier_lazy(root, storage_options)
        .filter(pl.col("S_COMMENT").str.contains(".*Customer.*Complaints.*"))
        .select(pl.col("S_SUPPKEY"), pl.col("S_SUPPKEY").alias("PS_SUPPKEY"))
    )

    var_1 = "Brand#45"

    q_final = (
        part_ds.join(part_supp_ds, left_on="P_PARTKEY", right_on="PS_PARTKEY")
        .filter(pl.col("P_BRAND") != var_1)
        .filter(pl.col("P_TYPE").str.contains("MEDIUM POLISHED*").not_())
        .filter(pl.col("P_SIZE").is_in([49, 14, 23, 45, 19, 3, 36, 9]))
        .join(supplier_ds, left_on="PS_SUPPKEY", right_on="S_SUPPKEY", how="left")
        .filter(pl.col("PS_SUPPKEY_RIGHT").is_null())
        .group_by(["P_BRAND", "P_TYPE", "P_SIZE"])
        .agg([pl.col("PS_SUPPKEY").n_unique().alias("SUPPLIER_CNT")])
        .sort(
            by=["SUPPLIER_CNT", "P_BRAND", "P_TYPE", "P_SIZE"],
            descending=[True, False, False, False],
        )
    )
    return q_final


def q17(root: str, storage_options: Dict):
    var_1 = "Brand#23"
    var_2 = "MED BOX"

    line_item_ds = load_lineitem_lazy(root, storage_options)
    part_ds = load_part_lazy(root, storage_options)

    res_1 = (
        part_ds.filter(pl.col("P_BRAND") == var_1)
        .filter(pl.col("P_CONTAINER") == var_2)
        .join(line_item_ds, how="left", left_on="P_PARTKEY", right_on="L_PARTKEY")
    ).cache()

    q_final = (
        res_1.group_by("P_PARTKEY")
        .agg((0.2 * pl.col("L_QUANTITY").mean()).alias("AVG_QUANTITY"))
        .select([pl.col("P_PARTKEY").alias("KEY"), pl.col("AVG_QUANTITY")])
        .join(res_1, left_on="KEY", right_on="P_PARTKEY")
        .filter(pl.col("L_QUANTITY") < pl.col("AVG_QUANTITY"))
        .select((pl.col("L_EXTENDEDPRICE").sum() / 7.0).round(2).alias("AVG_YEARLY"))
    )
    return q_final


def q18(root: str, storage_options: Dict):
    customer_ds = load_customer_lazy(root, storage_options)
    line_item_ds = load_lineitem_lazy(root, storage_options)
    orders_ds = load_orders_lazy(root, storage_options)

    var_1 = 300

    q_final = (
        line_item_ds.group_by("L_ORDERKEY")
        .agg(pl.col("L_QUANTITY").sum().alias("SUM_QUANTITY"))
        .filter(pl.col("SUM_QUANTITY") > var_1)
        .select([pl.col("L_ORDERKEY").alias("KEY"), pl.col("SUM_QUANTITY")])
        .join(orders_ds, left_on="KEY", right_on="O_ORDERKEY")
        .join(line_item_ds, left_on="KEY", right_on="L_ORDERKEY")
        .join(customer_ds, left_on="O_CUSTKEY", right_on="C_CUSTKEY")
        .group_by("C_NAME", "O_CUSTKEY", "KEY", "O_ORDERDATE", "O_TOTALPRICE")
        .agg(pl.col("L_QUANTITY").sum().alias("COL6"))
        .select(
            [
                pl.col("C_NAME"),
                pl.col("O_CUSTKEY").alias("C_CUSTKEY"),
                pl.col("KEY").alias("O_ORDERKEY"),
                pl.col("O_ORDERDATE").alias("O_ORDERDAT"),
                pl.col("O_TOTALPRICE"),
                pl.col("COL6"),
            ]
        )
        .sort(["O_TOTALPRICE", "O_ORDERDAT"], descending=[True, False])
        .limit(100)
    )
    return q_final


def q19(root: str, storage_options: Dict):
    line_item_ds = load_lineitem_lazy(root, storage_options)
    part_ds = load_part_lazy(root, storage_options)

    q_final = (
        part_ds.join(line_item_ds, left_on="P_PARTKEY", right_on="L_PARTKEY")
        .filter(pl.col("L_SHIPMODE").is_in(["AIR", "AIR REG"]))
        .filter(pl.col("L_SHIPINSTRUCT") == "DELIVER IN PERSON")
        .filter(
            (
                (pl.col("P_BRAND") == "Brand#12")
                & pl.col("P_CONTAINER").is_in(
                    ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]
                )
                & (pl.col("L_QUANTITY").is_between(1, 11))
                & (pl.col("P_SIZE").is_between(1, 5))
            )
            | (
                (pl.col("P_BRAND") == "Brand#23")
                & pl.col("P_CONTAINER").is_in(
                    ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]
                )
                & (pl.col("L_QUANTITY").is_between(10, 20))
                & (pl.col("P_SIZE").is_between(1, 10))
            )
            | (
                (pl.col("P_BRAND") == "Brand#34")
                & pl.col("P_CONTAINER").is_in(
                    ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]
                )
                & (pl.col("L_QUANTITY").is_between(20, 30))
                & (pl.col("P_SIZE").is_between(1, 15))
            )
        )
        .select(
            (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT")))
            .sum()
            .round(2)
            .alias("REVENUE")
        )
    )
    return q_final


def q20(root: str, storage_options: Dict):
    line_item_ds = load_lineitem_lazy(root, storage_options)
    nation_ds = load_nation_lazy(root, storage_options)
    supplier_ds = load_supplier_lazy(root, storage_options)
    part_ds = load_part_lazy(root, storage_options)
    part_supp_ds = load_partsupp_lazy(root, storage_options)

    var_1 = datetime(1994, 1, 1)
    var_2 = datetime(1995, 1, 1)
    var_3 = "CANADA"
    var_4 = "forest"

    res_1 = (
        line_item_ds.filter(
            pl.col("L_SHIPDATE").is_between(var_1, var_2, closed="left")
        )
        .group_by("L_PARTKEY", "L_SUPPKEY")
        .agg((pl.col("L_QUANTITY").sum() * 0.5).alias("SUM_QUANTITY"))
    )
    res_2 = nation_ds.filter(pl.col("N_NAME") == var_3)
    res_3 = supplier_ds.join(res_2, left_on="S_NATIONKEY", right_on="N_NATIONKEY")

    q_final = (
        part_ds.filter(pl.col("P_NAME").str.starts_with(var_4))
        .select(pl.col("P_PARTKEY").unique())
        .join(part_supp_ds, left_on="P_PARTKEY", right_on="PS_PARTKEY")
        .join(
            res_1,
            left_on=["PS_SUPPKEY", "P_PARTKEY"],
            right_on=["L_SUPPKEY", "L_PARTKEY"],
        )
        .filter(pl.col("PS_AVAILQTY") > pl.col("SUM_QUANTITY"))
        .select(pl.col("PS_SUPPKEY").unique())
        .join(res_3, left_on="PS_SUPPKEY", right_on="S_SUPPKEY")
        .with_columns(pl.col("S_ADDRESS").str.strip_chars())
        .select(["S_NAME", "S_ADDRESS"])
        .sort("S_NAME")
    )
    return q_final


def q21(root: str, storage_options: Dict):
    line_item_ds = load_lineitem_lazy(root, storage_options)
    supplier_ds = load_supplier_lazy(root, storage_options)
    nation_ds = load_nation_lazy(root, storage_options)
    orders_ds = load_orders_lazy(root, storage_options)

    var_1 = "SAUDI ARABIA"

    res_1 = (
        (
            line_item_ds.group_by("L_ORDERKEY")
            .agg(pl.col("L_SUPPKEY").n_unique().alias("NUNIQUE_COL"))
            .filter(pl.col("NUNIQUE_COL") > 1)
            .join(
                line_item_ds.filter(pl.col("L_RECEIPTDATE") > pl.col("L_COMMITDATE")),
                on="L_ORDERKEY",
            )
        )
    ).cache()

    q_final = (
        res_1.group_by("L_ORDERKEY")
        .agg(pl.col("L_SUPPKEY").n_unique().alias("NUNIQUE_COL"))
        .join(res_1, on="L_ORDERKEY")
        .join(supplier_ds, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
        .join(nation_ds, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        .join(orders_ds, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
        .filter(pl.col("NUNIQUE_COL") == 1)
        .filter(pl.col("N_NAME") == var_1)
        .filter(pl.col("O_ORDERSTATUS") == "F")
        .group_by("S_NAME")
        .agg(pl.count().alias("NUMWAIT"))
        .sort(by=["NUMWAIT", "S_NAME"], descending=[True, False])
        .limit(100)
    )
    return q_final


def q22(root: str, storage_options: Dict):
    orders_ds = load_orders_lazy(root, storage_options)
    customer_ds = load_customer_lazy(root, storage_options)

    res_1 = (
        customer_ds.with_columns(pl.col("C_PHONE").str.slice(0, 2).alias("CNTRYCODE"))
        .filter(pl.col("CNTRYCODE").str.contains("13|31|23|29|30|18|17"))
        .select(["C_ACCTBAL", "C_CUSTKEY", "CNTRYCODE"])
    )

    res_2 = (
        res_1.filter(pl.col("C_ACCTBAL") > 0.0)
        .select(pl.col("C_ACCTBAL").mean().alias("AVG_ACCTBAL"))
        .with_columns(pl.lit(1).alias("LIT"))
    )

    res_3 = orders_ds.select(pl.col("O_CUSTKEY").unique()).with_columns(
        pl.col("O_CUSTKEY").alias("C_CUSTKEY")
    )

    q_final = (
        res_1.join(res_3, on="C_CUSTKEY", how="left")
        .filter(pl.col("O_CUSTKEY").is_null())
        .with_columns(pl.lit(1).alias("LIT"))
        .join(res_2, on="LIT")
        .filter(pl.col("C_ACCTBAL") > pl.col("AVG_ACCTBAL"))
        .group_by("CNTRYCODE")
        .agg(
            [
                pl.col("C_ACCTBAL").count().alias("NUMCUST"),
                pl.col("C_ACCTBAL").sum().round(2).alias("TOTACCTBAL"),
            ]
        )
        .sort("CNTRYCODE")
    )
    return q_final


query_to_loaders = {
    1: [load_lineitem_lazy],
    2: [
        load_part_lazy,
        load_partsupp_lazy,
        load_supplier_lazy,
        load_nation_lazy,
        load_region_lazy,
    ],
    3: [load_lineitem_lazy, load_orders_lazy, load_customer_lazy],
    4: [load_lineitem_lazy, load_orders_lazy],
    5: [
        load_lineitem_lazy,
        load_orders_lazy,
        load_customer_lazy,
        load_nation_lazy,
        load_region_lazy,
        load_supplier_lazy,
    ],
    6: [load_lineitem_lazy],
    7: [
        load_lineitem_lazy,
        load_supplier_lazy,
        load_orders_lazy,
        load_customer_lazy,
        load_nation_lazy,
    ],
    8: [
        load_part_lazy,
        load_lineitem_lazy,
        load_supplier_lazy,
        load_orders_lazy,
        load_customer_lazy,
        load_nation_lazy,
        load_region_lazy,
    ],
    9: [
        load_lineitem_lazy,
        load_orders_lazy,
        load_part_lazy,
        load_nation_lazy,
        load_partsupp_lazy,
        load_supplier_lazy,
    ],
    10: [load_lineitem_lazy, load_orders_lazy, load_nation_lazy, load_customer_lazy],
    11: [load_partsupp_lazy, load_supplier_lazy, load_nation_lazy],
    12: [load_lineitem_lazy, load_orders_lazy],
    13: [load_customer_lazy, load_orders_lazy],
    14: [load_lineitem_lazy, load_part_lazy],
    15: [load_lineitem_lazy, load_supplier_lazy],
    16: [load_part_lazy, load_partsupp_lazy, load_supplier_lazy],
    17: [load_lineitem_lazy, load_part_lazy],
    18: [load_lineitem_lazy, load_orders_lazy, load_customer_lazy],
    19: [load_lineitem_lazy, load_part_lazy],
    20: [
        load_lineitem_lazy,
        load_part_lazy,
        load_nation_lazy,
        load_partsupp_lazy,
        load_supplier_lazy,
    ],
    21: [load_lineitem_lazy, load_orders_lazy, load_supplier_lazy, load_nation_lazy],
    22: [load_customer_lazy, load_orders_lazy],
}

query_to_runner = {
    1: q01,
    2: q02,
    3: q03,
    4: q04,
    5: q05,
    6: q06,
    7: q07,
    8: q08,
    9: q09,
    10: q10,
    11: q11,
    12: q12,
    13: q13,
    14: q14,
    15: q15,
    16: q16,
    17: q17,
    18: q18,
    19: q19,
    20: q20,
    21: q21,
    22: q22,
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

    queries = list(range(1, 23))
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
