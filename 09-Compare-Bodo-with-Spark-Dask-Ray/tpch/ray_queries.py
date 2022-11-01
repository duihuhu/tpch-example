"""
This code is adapted from 
https://github.com/Bodo-inc/Bodo-examples/blob/master/examples/tpch/bodo_queries.py

The differences are in:
1. Query 1 : double square brackets is required in gb = lineitem_filtered.groupby()
2. Query 8 : total.groupby("O_YEAR", as_index = False).apply(udf) --> total.groupby("O_YEAR").apply(udf).reset_index()

"""

import ray
import modin.pandas as pd
import time
#ray.init()
ray.init(address = "auto")



def run_queries(data_folder):
    # Load the data
    t1 = time.time()
    lineitem = load_lineitem(data_folder)
    orders = load_orders(data_folder)
    customer = load_customer(data_folder)
    nation = load_nation(data_folder)
    region = load_region(data_folder)
    supplier = load_supplier(data_folder)
    part = load_part(data_folder)
    partsupp = load_partsupp(data_folder)
    print("hucc Reading time (s): ", time.time())
    # Run the Queries:
    # q01
    q01(lineitem)
    '''
    # q2
    q02(part, partsupp, supplier, nation, region)
    # q03
    
    q03(lineitem, orders, customer)
    # q04
    q04(lineitem, orders)
    # q05
    q05(lineitem, orders, customer, nation, region, supplier)

    # q06
    q06(lineitem)
    # q07
    q07(lineitem, supplier, orders, customer, nation)
    # q08
    q08(part, lineitem, supplier, orders, customer, nation, region)
    # q09
    q09(lineitem, orders, part, nation, partsupp, supplier)
    # q10
    q10(lineitem, orders, customer, nation)
    # q11
    q11(partsupp, supplier, nation)
    # q12
    q12(lineitem, orders)
    # q13
    q13(customer, orders)
    # q14
    q14(lineitem, part)
    # q15
    q15(lineitem, supplier)
    # q16
    q16(part, partsupp, supplier)
    # q17
    q17(lineitem, part)
    # q18
    q18(lineitem, orders, customer)
    # q19
    q19(lineitem, part)
    # q20
    q20(lineitem, part, nation, partsupp, supplier)
    # q21
    q21(lineitem, orders, supplier, nation)
    # q22
    q22(customer, orders)
    print("Total Query time (s): ", time.time() - t1)
    '''

def load_lineitem(data_folder):
    data_path = data_folder + "/lineitem.pq"
    df = pd.read_parquet(
        data_path,
    )
    return df

def load_part(data_folder):
    data_path = data_folder + "/part.pq"
    df = pd.read_parquet(
        data_path,
    )
    return df


def load_orders(data_folder):
    data_path = data_folder + "/orders.pq"
    df = pd.read_parquet(
        data_path,
    )
    return df


def load_customer(data_folder):
    data_path = data_folder + "/customer.pq"
    df = pd.read_parquet(
        data_path,
    )
    return df


def load_nation(data_folder):
    data_path = data_folder + "/nation.pq"
    df = pd.read_parquet(
        data_path,
    )
    return df


def load_region(data_folder):
    data_path = data_folder + "/region.pq"
    df = pd.read_parquet(
        data_path,
    )
    return df


def load_supplier(data_folder):
    data_path = data_folder + "/supplier.pq"
    df = pd.read_parquet(
        data_path,
    )
    return df


def load_partsupp(data_folder):
    data_path = data_folder + "/partsupp.pq"
    df = pd.read_parquet(
        data_path,
    )
    return df


def q01(lineitem):
    date = pd.Timestamp("1998-09-02")
    # print(list(lineitem.columns.values))
    t1 = time.time()
    lineitem_filtered = lineitem.loc[:, ["L_ORDERKEY", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS",  "L_SHIPDATE"]]
    t2 = time.time()
    print("t2: ", t2)
    sel = lineitem_filtered.L_SHIPDATE <= date
    t3 = time.time()
    print("t3: ", t3)
    lineitem_filtered = lineitem_filtered[sel]
    t4 = time.time()
    print("t4: ", t4)
    lineitem_filtered["AVG_QTY"] = lineitem_filtered.L_QUANTITY
    t5 = time.time()
    print("t5: ", t5)
    lineitem_filtered["AVG_PRICE"] = lineitem_filtered.L_EXTENDEDPRICE
    t6 = time.time()
    print("t6: ", t6)
    lineitem_filtered["DISC_PRICE"] = lineitem_filtered.L_EXTENDEDPRICE * (1 - lineitem_filtered.L_DISCOUNT)
    t7 = time.time()
    print("t7: ", t7)
    lineitem_filtered["CHARGE"] = (
        lineitem_filtered.L_EXTENDEDPRICE * (1 - lineitem_filtered.L_DISCOUNT) * (1 + lineitem_filtered.L_TAX)
    )
    #ray needs double square bracket
    t8 = time.time()
    print("t8: ", t8)
    gb = lineitem_filtered.groupby(["L_RETURNFLAG", "L_LINESTATUS"], as_index=False)[[
        "L_ORDERKEY",
        "L_QUANTITY",
        "L_EXTENDEDPRICE",
        "L_DISCOUNT",
        "AVG_QTY",
        "AVG_PRICE",
        "CHARGE",
        "DISC_PRICE",
    ]]
    t9 = time.time()
    print("t9: ", t9)
    total = gb.agg(
        {
            "L_QUANTITY": "sum",
            "L_EXTENDEDPRICE": "sum",
            "DISC_PRICE": "sum",
            "CHARGE": "sum",
            "AVG_QTY": "mean",
            "AVG_PRICE": "mean",
            "L_DISCOUNT": "mean",
            "L_ORDERKEY": "count",
        }
    )
    t10 = time.time()
    print("t10: ", t10)
    total = total.sort_values(["L_RETURNFLAG", "L_LINESTATUS"])
    print(total)
    t11 = time.time()
    print("Q01 Execution time (s): ", time.time() - t1)
    print("%.3f" % (t2-t1),"%.3f" % (t3-t2),"%.3f" % (t4-t3),"%.3f" % (t5-t4),"%.3f" % (t6-t5),"%.3f" % (t7-t6),"%.3f" % (t8-t7),"%.3f" % (t9-t8),"%.3f" % (t10-t9), "%.3f" % (t11-t10))



def q02(part, partsupp, supplier, nation, region):
    t1 = time.time()
    nation_filtered = nation.loc[:, ["N_NATIONKEY", "N_NAME", "N_REGIONKEY"]]
    t2 = time.time()
    region_filtered = region[(region["R_NAME"] == "EUROPE")]
    t3 = time.time()
    region_filtered = region_filtered.loc[:, ["R_REGIONKEY"]]
    t4 = time.time()
    r_n_merged = nation_filtered.merge(region_filtered, left_on='N_REGIONKEY', right_on='R_REGIONKEY', how='inner')
    t5 = time.time()
    r_n_merged = r_n_merged.loc[:, ["N_NATIONKEY", "N_NAME"]]
    t6 = time.time()
    supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"]]
    t7 = time.time()
    s_r_n_merged = r_n_merged.merge(supplier_filtered, left_on="N_NATIONKEY", right_on="S_NATIONKEY", how="inner")
    t8 = time.time()
    s_r_n_merged = s_r_n_merged.loc[:, ["N_NAME", "S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE", "S_ACCTBAL", "S_COMMENT"]]
    t9 = time.time()
    partsupp_filtered = partsupp.loc[:, ["PS_PARTKEY", "PS_SUPPKEY", "PS_SUPPLYCOST"]]
    t10 = time.time()
    ps_s_r_n_merged = s_r_n_merged.merge(partsupp_filtered, left_on="S_SUPPKEY", right_on="PS_SUPPKEY", how="inner")
    t11 = time.time()
    ps_s_r_n_merged = ps_s_r_n_merged.loc[:, ["N_NAME", "S_NAME", "S_ADDRESS", "S_PHONE", "S_ACCTBAL", "S_COMMENT", "PS_PARTKEY", "PS_SUPPLYCOST"]]
    t12 = time.time()
    part_filtered = part.loc[:, ["P_PARTKEY", "P_MFGR", "P_SIZE", "P_TYPE"]]
    t13 = time.time()
    part_filtered = part_filtered[(part_filtered["P_SIZE"] == 15) & (part_filtered["P_TYPE"].str.endswith("BRASS"))]
    t14 = time.time()
    part_filtered = part_filtered.loc[:, ["P_PARTKEY", "P_MFGR"]]
    t15 = time.time()
    merged_df = part_filtered.merge(ps_s_r_n_merged, left_on='P_PARTKEY', right_on='PS_PARTKEY', how='inner')
    t16 = time.time()
    merged_df = merged_df.loc[:, ["N_NAME", "S_NAME", "S_ADDRESS", "S_PHONE", "S_ACCTBAL", "S_COMMENT", "PS_SUPPLYCOST", "P_PARTKEY", "P_MFGR"]]
    t17 = time.time()
    min_values = merged_df.groupby("P_PARTKEY", as_index=False)["PS_SUPPLYCOST"].min()
    t18 = time.time()
    min_values.columns=["P_PARTKEY_CPY", "MIN_SUPPLYCOST"]
    t19 = time.time()
    merged_df = merged_df.merge(min_values, left_on=["P_PARTKEY", "PS_SUPPLYCOST"], right_on=["P_PARTKEY_CPY", "MIN_SUPPLYCOST"], how="inner")
    t20 = time.time()
    total = merged_df.loc[:, ["S_ACCTBAL", "S_NAME", "N_NAME", "P_PARTKEY", "P_MFGR", "S_ADDRESS", "S_PHONE", "S_COMMENT"]]
    t21 = time.time()
    total = total.sort_values(by=["S_ACCTBAL","N_NAME","S_NAME","P_PARTKEY",], ascending=[False,True,True,True,])
    t22 = time.time()
    # print(total)
    print("Q02 Execution time (s): ", t22 - t1)
    print("%.3f" % (t2-t1),"%.3f" % (t3-t2),"%.3f" % (t4-t3),"%.3f" % (t5-t4),"%.3f" % (t6-t5),"%.3f" % (t7-t6),"%.3f" % (t8-t7),"%.3f" % (t9-t8),"%.3f" % (t10-t9),"%.3f" % (t11-t10),"%.3f" % (t12-t11),"%.3f" % (t13-t12),"%.3f" % (t14-t13),"%.3f" % (t15-t14),"%.3f" % (t16-t15),"%.3f" % (t17-t16),"%.3f" % (t18-t17),"%.3f" % (t19-t18),"%.3f" % (t20-t19),"%.3f" % (t21-t20),"%.3f" % (t22-t21),)


def q03(lineitem, orders, customer):
    t1 = time.time()
    date = pd.Timestamp("1995-03-04")
    lineitem_filtered = lineitem.loc[:, ["L_ORDERKEY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_SHIPDATE"]]
    orders_filtered = orders.loc[:, ["O_ORDERKEY", "O_CUSTKEY", "O_ORDERDATE", "O_SHIPPRIORITY"]]
    customer_filtered = customer.loc[:, ["C_MKTSEGMENT", "C_CUSTKEY"]]
    lsel = lineitem_filtered.L_SHIPDATE > date
    osel = orders_filtered.O_ORDERDATE < date
    csel = customer_filtered.C_MKTSEGMENT == "HOUSEHOLD"
    flineitem = lineitem_filtered[lsel]
    forders = orders_filtered[osel]
    fcustomer = customer_filtered[csel]
    jn1 = fcustomer.merge(forders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
    jn2 = jn1.merge(flineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
    jn2["TMP"] = jn2.L_EXTENDEDPRICE * (1 - jn2.L_DISCOUNT)
    total = (
        jn2.groupby(["L_ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY"], as_index=False)[
            "TMP"
        ]
        .sum()
        .sort_values(["TMP"], ascending=False)
    )
    res = total.loc[:, ["L_ORDERKEY", "TMP", "O_ORDERDATE", "O_SHIPPRIORITY"]]
    print(res.head(10))
    print("Q03 Execution time (s): ", time.time() - t1)



def q04(lineitem, orders):
    t1 = time.time()
    date1 = pd.Timestamp("1993-11-01")
    date2 = pd.Timestamp("1993-08-01")
    lsel = lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE
    osel = (orders.O_ORDERDATE < date1) & (orders.O_ORDERDATE >= date2)
    flineitem = lineitem[lsel]
    forders = orders[osel]
    jn = forders[forders["O_ORDERKEY"].isin(flineitem["L_ORDERKEY"])]
    total = (
        jn.groupby("O_ORDERPRIORITY", as_index=False)["O_ORDERKEY"]
        .count()
        .sort_values(["O_ORDERPRIORITY"])
    )
    print(total)
    print("Q04 Execution time (s): ", time.time() - t1)



def q05(lineitem, orders, customer, nation, region, supplier):
    t1 = time.time()
    date1 = pd.Timestamp("1996-01-01")
    date2 = pd.Timestamp("1997-01-01")
    rsel = region.R_NAME == "ASIA"
    osel = (orders.O_ORDERDATE >= date1) & (orders.O_ORDERDATE < date2)
    forders = orders[osel]
    fregion = region[rsel]
    jn1 = fregion.merge(nation, left_on="R_REGIONKEY", right_on="N_REGIONKEY")
    jn2 = jn1.merge(customer, left_on="N_NATIONKEY", right_on="C_NATIONKEY")
    jn3 = jn2.merge(forders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
    jn4 = jn3.merge(lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
    jn5 = supplier.merge(
        jn4, left_on=["S_SUPPKEY", "S_NATIONKEY"], right_on=["L_SUPPKEY", "N_NATIONKEY"]
    )
    jn5["TMP"] = jn5.L_EXTENDEDPRICE * (1.0 - jn5.L_DISCOUNT)
    gb = jn5.groupby("N_NAME", as_index=False)["TMP"].sum()
    total = gb.sort_values("TMP", ascending=False)
    print("Q05 Execution time (s): ", time.time() - t1)



def q06(lineitem):
    t1 = time.time()
    date1 = pd.Timestamp("1996-01-01")
    date2 = pd.Timestamp("1997-01-01")
    lineitem_filtered = lineitem.loc[:, ["L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_SHIPDATE"]] #loc是指筛选某几列
    t2 = time.time()
    sel = (
        (lineitem_filtered.L_SHIPDATE >= date1)
        & (lineitem_filtered.L_SHIPDATE < date2)
        & (lineitem_filtered.L_DISCOUNT >= 0.08)
        & (lineitem_filtered.L_DISCOUNT <= 0.1)
        & (lineitem_filtered.L_QUANTITY < 24)
    )
    t3 = time.time()
    flineitem = lineitem_filtered[sel]
    t4 = time.time()
    total = (flineitem.L_EXTENDEDPRICE * flineitem.L_DISCOUNT).sum()
    t5 = time.time()
    # print(total)
    print("Q06 Execution time (s): ", time.time() - t1)
    print("%.3f" % (t2-t1),"%.3f" % (t3-t2),"%.3f" % (t4-t3),"%.3f" % (t5-t4),)


def q07(lineitem, supplier, orders, customer, nation):
    """ This version is faster than q07_old. Keeping the old one for reference """
    t1 = time.time()

    lineitem_filtered = lineitem[(lineitem["L_SHIPDATE"] >= pd.Timestamp("1995-01-01")) & (lineitem["L_SHIPDATE"] < pd.Timestamp("1997-01-01"))]
    lineitem_filtered["L_YEAR"] = lineitem_filtered["L_SHIPDATE"].apply(lambda x: x.year)
    lineitem_filtered["VOLUME"] = lineitem_filtered["L_EXTENDEDPRICE"] * (1.0 - lineitem_filtered["L_DISCOUNT"])
    lineitem_filtered = lineitem_filtered.loc[:, ["L_ORDERKEY", "L_SUPPKEY", "L_YEAR", "VOLUME"]]
    supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NATIONKEY"]]
    orders_filtered = orders.loc[:, ["O_ORDERKEY", "O_CUSTKEY"]]
    customer_filtered = customer.loc[:, ["C_CUSTKEY", "C_NATIONKEY"]]
    n1 = nation[(nation["N_NAME"] == "FRANCE")].loc[:, ["N_NATIONKEY", "N_NAME"]]
    n2 = nation[(nation["N_NAME"] == "GERMANY")].loc[:, ["N_NATIONKEY", "N_NAME"]]

    # ----- do nation 1 -----
    N1_C = customer_filtered.merge(n1, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='inner')
    N1_C = N1_C.drop(columns=["C_NATIONKEY", "N_NATIONKEY"]).rename(columns={"N_NAME": "CUST_NATION"})
    N1_C_O = N1_C.merge(orders_filtered, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='inner')
    N1_C_O = N1_C_O.drop(columns=["C_CUSTKEY", "O_CUSTKEY"])

    # NOTE: this is faster than first merging lineitem with N1_C_O
    N2_S = supplier_filtered.merge(n2, left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='inner')
    N2_S = N2_S.drop(columns=["S_NATIONKEY", "N_NATIONKEY"]).rename(columns={"N_NAME": "SUPP_NATION"})
    N2_S_L = N2_S.merge(lineitem_filtered, left_on='S_SUPPKEY', right_on='L_SUPPKEY', how='inner')
    N2_S_L = N2_S_L.drop(columns=["S_SUPPKEY", "L_SUPPKEY"])

    total1 = N1_C_O.merge(N2_S_L, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
    total1 = total1.drop(columns=["O_ORDERKEY", "L_ORDERKEY"])

    # ----- do nation 2 ----- (same as nation 1 section but with nation 2)
    N2_C = customer_filtered.merge(n2, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='inner')
    N2_C = N2_C.drop(columns=["C_NATIONKEY", "N_NATIONKEY"]).rename(columns={"N_NAME": "CUST_NATION"})
    N2_C_O = N2_C.merge(orders_filtered, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='inner')
    N2_C_O = N2_C_O.drop(columns=["C_CUSTKEY", "O_CUSTKEY"])

    N1_S = supplier_filtered.merge(n1, left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='inner')
    N1_S = N1_S.drop(columns=["S_NATIONKEY", "N_NATIONKEY"]).rename(columns={"N_NAME": "SUPP_NATION"})
    N1_S_L = N1_S.merge(lineitem_filtered, left_on='S_SUPPKEY', right_on='L_SUPPKEY', how='inner')
    N1_S_L = N1_S_L.drop(columns=["S_SUPPKEY", "L_SUPPKEY"])

    total2 = N2_C_O.merge(N1_S_L, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
    total2 = total2.drop(columns=["O_ORDERKEY", "L_ORDERKEY"])

    # concat results
    total = pd.concat([total1, total2])

    total = total.groupby(["SUPP_NATION", "CUST_NATION", "L_YEAR"], as_index = False).agg(REVENUE=pd.NamedAgg(column="VOLUME", aggfunc="sum"))
    total = total.sort_values(by=["SUPP_NATION","CUST_NATION","L_YEAR"], ascending=[True,True,True,])
    print(total)
    print("Q07 Execution time (s): ", time.time() - t1)



def q08(part, lineitem, supplier, orders, customer, nation, region):
    t1 = time.time()
    part_filtered = part[(part["P_TYPE"] == "ECONOMY ANODIZED STEEL")]
    part_filtered = part_filtered.loc[:, ["P_PARTKEY"]]
    lineitem_filtered = lineitem.loc[:, ["L_PARTKEY", "L_SUPPKEY", "L_ORDERKEY"]]
    lineitem_filtered["VOLUME"] = lineitem["L_EXTENDEDPRICE"] * (1.0 - lineitem["L_DISCOUNT"])
    total = part_filtered.merge(lineitem_filtered, left_on="P_PARTKEY", right_on="L_PARTKEY", how="inner")
    total = total.loc[:, ["L_SUPPKEY", "L_ORDERKEY", "VOLUME"]]
    supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NATIONKEY"]]
    total = total.merge(supplier_filtered, left_on="L_SUPPKEY", right_on="S_SUPPKEY", how="inner")
    total = total.loc[:, ["L_ORDERKEY", "VOLUME", "S_NATIONKEY"]]
    orders_filtered = orders[(orders["O_ORDERDATE"] >= pd.Timestamp("1995-01-01")) & (orders["O_ORDERDATE"] < pd.Timestamp("1997-01-01"))]
    orders_filtered["O_YEAR"] = orders_filtered["O_ORDERDATE"].apply(lambda x: x.year)
    orders_filtered = orders_filtered.loc[:, ["O_ORDERKEY", "O_CUSTKEY", "O_YEAR"]]
    total = total.merge(orders_filtered, left_on="L_ORDERKEY", right_on="O_ORDERKEY", how="inner")
    total = total.loc[:, ["VOLUME", "S_NATIONKEY", "O_CUSTKEY", "O_YEAR"]]
    customer_filtered = customer.loc[:, ["C_CUSTKEY", "C_NATIONKEY"]]
    total = total.merge(customer_filtered, left_on="O_CUSTKEY", right_on="C_CUSTKEY", how="inner")
    total = total.loc[:, ["VOLUME", "S_NATIONKEY", "O_YEAR", "C_NATIONKEY"]]
    n1_filtered = nation.loc[:, ["N_NATIONKEY", "N_REGIONKEY"]]
    n2_filtered = nation.loc[:, ["N_NATIONKEY", "N_NAME"]].rename(columns={"N_NAME": "NATION"})
    total = total.merge(n1_filtered, left_on="C_NATIONKEY", right_on="N_NATIONKEY", how="inner")
    total = total.loc[:, ["VOLUME", "S_NATIONKEY", "O_YEAR", "N_REGIONKEY"]]
    total = total.merge(n2_filtered, left_on="S_NATIONKEY", right_on="N_NATIONKEY", how="inner")
    total = total.loc[:, ["VOLUME", "O_YEAR", "N_REGIONKEY", "NATION"]]
    region_filtered = region[(region["R_NAME"] == "AMERICA")]
    region_filtered = region_filtered.loc[:, ["R_REGIONKEY"]]
    total = total.merge(region_filtered, left_on="N_REGIONKEY", right_on="R_REGIONKEY", how="inner")
    total = total.loc[:, ["VOLUME", "O_YEAR", "NATION"]]

    def udf(df):
        demonimator = df["VOLUME"].sum()
        df = df[df["NATION"] == "BRAZIL"]
        numerator = df["VOLUME"].sum()
        return numerator / demonimator

    #modin returns empty column with as_index=false
    total = total.groupby("O_YEAR").apply(udf).reset_index()
    total.columns = ["O_YEAR", "MKT_SHARE"]
    total = total.sort_values(by=["O_YEAR",], ascending=[True,])
    print(total)
    print("Q08 Execution time (s): ", time.time() - t1)



def q09(lineitem, orders, part, nation, partsupp, supplier):
    t1 = time.time()
    psel = part.P_NAME.str.contains("ghost")
    fpart = part[psel]
    jn1 = lineitem.merge(fpart, left_on="L_PARTKEY", right_on="P_PARTKEY")
    jn2 = jn1.merge(supplier, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
    jn3 = jn2.merge(nation, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
    jn4 = partsupp.merge(
        jn3, left_on=["PS_PARTKEY", "PS_SUPPKEY"], right_on=["L_PARTKEY", "L_SUPPKEY"]
    )
    jn5 = jn4.merge(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
    jn5["TMP"] = jn5.L_EXTENDEDPRICE * (1 - jn5.L_DISCOUNT) - (
        (1 * jn5.PS_SUPPLYCOST) * jn5.L_QUANTITY
    )
    jn5["O_YEAR"] = jn5.O_ORDERDATE.apply(lambda x: x.year)
    gb = jn5.groupby(["N_NAME", "O_YEAR"], as_index=False)["TMP"].sum()
    total = gb.sort_values(["N_NAME", "O_YEAR"], ascending=[True, False])
    print(total)
    print("Q09 Execution time (s): ", time.time() - t1)



def q10(lineitem, orders, customer, nation):
    t1 = time.time()
    date1 = pd.Timestamp("1994-11-01")
    date2 = pd.Timestamp("1995-02-01")
    osel = (orders.O_ORDERDATE >= date1) & (orders.O_ORDERDATE < date2)
    lsel = lineitem.L_RETURNFLAG == "R"
    forders = orders[osel]
    flineitem = lineitem[lsel]
    jn1 = flineitem.merge(forders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
    jn2 = jn1.merge(customer, left_on="O_CUSTKEY", right_on="C_CUSTKEY")
    jn3 = jn2.merge(nation, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
    jn3["TMP"] = jn3.L_EXTENDEDPRICE * (1.0 - jn3.L_DISCOUNT)
    gb = jn3.groupby(
        [
            "C_CUSTKEY",
            "C_NAME",
            "C_ACCTBAL",
            "C_PHONE",
            "N_NAME",
            "C_ADDRESS",
            "C_COMMENT",
        ],
        as_index=False,
    )["TMP"].sum()
    total = gb.sort_values("TMP", ascending=False)
    print(total.head(20))
    print("Q10 Execution time (s): ", time.time() - t1)


def q11(partsupp, supplier, nation):
    t1 = time.time()
    partsupp_filtered = partsupp.loc[:, ["PS_PARTKEY", "PS_SUPPKEY"]]
    partsupp_filtered["TOTAL_COST"] = partsupp["PS_SUPPLYCOST"] * partsupp["PS_AVAILQTY"]
    supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NATIONKEY"]]
    ps_supp_merge = partsupp_filtered.merge(supplier_filtered, left_on='PS_SUPPKEY', right_on='S_SUPPKEY', how='inner')
    ps_supp_merge.loc[:, ["PS_PARTKEY", "S_NATIONKEY", "TOTAL_COST"]]
    nation_filtered = nation[(nation["N_NAME"] == "GERMANY")]
    nation_filtered = nation_filtered.loc[:, ["N_NATIONKEY"]]
    ps_supp_n_merge = ps_supp_merge.merge(nation_filtered, left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='inner')
    ps_supp_n_merge = ps_supp_n_merge.loc[:, ["PS_PARTKEY", "TOTAL_COST"]]
    sum_val = ps_supp_n_merge["TOTAL_COST"].sum() * 0.0001
    total = ps_supp_n_merge.groupby(["PS_PARTKEY"], as_index = False).agg(VALUE=pd.NamedAgg(column="TOTAL_COST", aggfunc="sum"))
    total = total[total["VALUE"] > sum_val]
    total = total.sort_values("VALUE", ascending=False)
    print(total)
    print("Q11 Execution time (s): ", time.time() - t1)



def q12(lineitem, orders):
    t1 = time.time()
    date1 = pd.Timestamp("1994-01-01")
    date2 = pd.Timestamp("1995-01-01")
    sel = (
        (lineitem.L_RECEIPTDATE < date2)
        & (lineitem.L_COMMITDATE < date2)
        & (lineitem.L_SHIPDATE < date2)
        & (lineitem.L_SHIPDATE < lineitem.L_COMMITDATE)
        & (lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE)
        & (lineitem.L_RECEIPTDATE >= date1)
        & ((lineitem.L_SHIPMODE == "MAIL") | (lineitem.L_SHIPMODE == "SHIP"))
    )
    flineitem = lineitem[sel]
    jn = flineitem.merge(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")

    def g1(x):
        return ((x == "1-URGENT") | (x == "2-HIGH")).sum()

    def g2(x):
        return ((x != "1-URGENT") & (x != "2-HIGH")).sum()

    total = jn.groupby("L_SHIPMODE", as_index=False)["O_ORDERPRIORITY"].agg((g1, g2))
    total = total.sort_values("L_SHIPMODE")
    print(total)
    print("Q12 Execution time (s): ", time.time() - t1)



def q13(customer, orders):
    t1 = time.time()
    customer_filtered = customer.loc[:, ["C_CUSTKEY"]]
    orders_filtered = orders[~orders["O_COMMENT"].str.contains("special(\S|\s)*requests")]
    orders_filtered = orders_filtered.loc[:, ["O_ORDERKEY", "O_CUSTKEY"]]
    c_o_merged = customer_filtered.merge(orders_filtered, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')
    c_o_merged = c_o_merged.loc[:, ["C_CUSTKEY", "O_ORDERKEY"]]
    count_df = c_o_merged.groupby(["C_CUSTKEY"], as_index=False).agg(C_COUNT=pd.NamedAgg(column="O_ORDERKEY", aggfunc="count"))
    total = count_df.groupby(["C_COUNT"], as_index = False).size()
    total.columns = ["C_COUNT","CUSTDIST"]
    total = total.sort_values(by=["CUSTDIST","C_COUNT"], ascending=[False,False,])
    print(total)
    print("Q13 Execution time (s): ", time.time() - t1)



def q14(lineitem, part):
    t1 = time.time()
    startDate = pd.Timestamp("1994-03-01")
    endDate = pd.Timestamp("1994-04-01")
    p_type_like = "PROMO"
    part_filtered = part.loc[:, ["P_PARTKEY", "P_TYPE"]]
    t2 = time.time()
    lineitem_filtered = lineitem.loc[:, ["L_EXTENDEDPRICE", "L_DISCOUNT", "L_SHIPDATE", "L_PARTKEY"]]
    t3 = time.time()
    sel = (lineitem_filtered.L_SHIPDATE >= startDate) & (lineitem_filtered.L_SHIPDATE < endDate)
    t4 = time.time()
    flineitem = lineitem_filtered[sel]
    t5 = time.time()
    jn = flineitem.merge(part_filtered, left_on="L_PARTKEY", right_on="P_PARTKEY")
    t6 = time.time()
    jn["TMP"] = jn.L_EXTENDEDPRICE * (1.0 - jn.L_DISCOUNT)
    total = jn[jn.P_TYPE.str.startswith(p_type_like)].TMP.sum() * 100 / jn.TMP.sum()
    t7 = time.time()
    print(total)
    t8 = time.time()
    print("Q14 Execution time (s): ", time.time() - t1)
    print("%.3f" % (t2-t1),"%.3f" % (t3-t2),"%.3f" % (t4-t3),"%.3f" % (t5-t4),"%.3f" % (t6-t5),"%.3f" % (t7-t6),"%.3f" % (t8-t7),)



def q15(lineitem, supplier):
    t1 = time.time()
    lineitem_filtered = lineitem[(lineitem["L_SHIPDATE"] >= pd.Timestamp('1996-01-01')) & (lineitem["L_SHIPDATE"] < (pd.Timestamp('1996-01-01') + pd.DateOffset(months=3)))]
    lineitem_filtered["REVENUE_PARTS"] = lineitem_filtered["L_EXTENDEDPRICE"] * (1.0 - lineitem_filtered["L_DISCOUNT"])
    lineitem_filtered = lineitem_filtered.loc[:, ["L_SUPPKEY", "REVENUE_PARTS"]]
    revenue_table = lineitem_filtered.groupby("L_SUPPKEY", as_index=False).agg(TOTAL_REVENUE=pd.NamedAgg(column="REVENUE_PARTS", aggfunc="sum")).rename(columns={"L_SUPPKEY": "SUPPLIER_NO"}, copy=False)
    max_revenue = revenue_table["TOTAL_REVENUE"].max()
    revenue_table = revenue_table[revenue_table["TOTAL_REVENUE"] == max_revenue]
    supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE"]]
    total = supplier_filtered.merge(revenue_table, left_on="S_SUPPKEY", right_on="SUPPLIER_NO", how="inner")
    total = total.loc[:, ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE", "TOTAL_REVENUE"]]
    print(total)
    print("Q15 Execution time (s): ", time.time() - t1)



def q16(part, partsupp, supplier):
    t1 = time.time()
    part_filtered = part[
                      (part["P_BRAND"] != "Brand#45")
                      & (~part["P_TYPE"].str.contains("^MEDIUM POLISHED"))
                      & part["P_SIZE"].isin([49, 14, 23, 45, 19, 3, 36, 9])
                    ]
    part_filtered = part_filtered.loc[:, ["P_PARTKEY", "P_BRAND", "P_TYPE", "P_SIZE"]]
    partsupp_filtered = partsupp.loc[:, ["PS_PARTKEY", "PS_SUPPKEY"]]
    total = part_filtered.merge(partsupp_filtered, left_on="P_PARTKEY", right_on="PS_PARTKEY", how="inner")
    total = total.loc[:, ["P_BRAND", "P_TYPE", "P_SIZE", "PS_SUPPKEY"]]
    supplier_filtered = supplier[supplier["S_COMMENT"].str.contains("Customer(\S|\s)*Complaints")]
    supplier_filtered = supplier_filtered.loc[:, ["S_SUPPKEY"]].drop_duplicates()
    # left merge to select only ps_suppkey values not in supplier_filtered
    total = total.merge(supplier_filtered, left_on="PS_SUPPKEY", right_on="S_SUPPKEY", how="left")
    total = total[total["S_SUPPKEY"].isna()]
    total = total.loc[:, ["P_BRAND", "P_TYPE", "P_SIZE", "PS_SUPPKEY"]]
    total = total.groupby(["P_BRAND", "P_TYPE", "P_SIZE"], as_index=False)["PS_SUPPKEY"].nunique()
    total.columns = ["P_BRAND", "P_TYPE", "P_SIZE", "SUPPLIER_CNT"]
    total = total.sort_values(by=["SUPPLIER_CNT", "P_BRAND", "P_TYPE", "P_SIZE"], ascending=[False, True, True, True])
    print(total)
    print("Q16 Execution time (s): ", time.time() - t1)



def q17(lineitem, part):
    t1 = time.time()
    left = lineitem.loc[:, ["L_PARTKEY", "L_QUANTITY", "L_EXTENDEDPRICE"]]
    right = part[((part["P_BRAND"] == "Brand#23") & (part["P_CONTAINER"] == "MED BOX"))]
    right = right.loc[:, ["P_PARTKEY"]]
    line_part_merge = left.merge(right, left_on='L_PARTKEY', right_on='P_PARTKEY', how='inner')
    line_part_merge = line_part_merge.loc[:, ["L_QUANTITY", "L_EXTENDEDPRICE", "P_PARTKEY"]]
    lineitem_filtered = lineitem.loc[:, ["L_PARTKEY", "L_QUANTITY"]]
    lineitem_avg = lineitem_filtered.groupby(["L_PARTKEY"], as_index = False).agg(avg=pd.NamedAgg(column="L_QUANTITY", aggfunc="mean"))
    lineitem_avg["avg"] = 0.2 * lineitem_avg["avg"]
    lineitem_avg = lineitem_avg.loc[:, ["L_PARTKEY", "avg"]]
    total = line_part_merge.merge(lineitem_avg, left_on='P_PARTKEY', right_on='L_PARTKEY', how='inner')
    total = total[total["L_QUANTITY"] < total["avg"]]
    total = pd.DataFrame({"avg_yearly": [total["L_EXTENDEDPRICE"].sum() / 7.0]})
    print(total)
    print("Q17 Execution time (s): ", time.time() - t1)



def q18(lineitem, orders, customer):
    t1 = time.time()
    gb1 = lineitem.groupby("L_ORDERKEY", as_index=False)["L_QUANTITY"].sum()
    fgb1 = gb1[gb1.L_QUANTITY > 300]
    jn1 = fgb1.merge(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
    jn2 = jn1.merge(customer, left_on="O_CUSTKEY", right_on="C_CUSTKEY")
    gb2 = jn2.groupby(
        ["C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE"],
        as_index=False,
    )["L_QUANTITY"].sum()
    total = gb2.sort_values(["O_TOTALPRICE", "O_ORDERDATE"], ascending=[False, True])
    print(total.head(100))
    print("Q18 Execution time (s): ", time.time() - t1)



def q19(lineitem, part):
    t1 = time.time()
    Brand31 = "Brand#31"
    Brand43 = "Brand#43"
    SMBOX = "SM BOX"
    SMCASE = "SM CASE"
    SMPACK = "SM PACK"
    SMPKG = "SM PKG"
    MEDBAG = "MED BAG"
    MEDBOX = "MED BOX"
    MEDPACK = "MED PACK"
    MEDPKG = "MED PKG"
    LGBOX = "LG BOX"
    LGCASE = "LG CASE"
    LGPACK = "LG PACK"
    LGPKG = "LG PKG"
    DELIVERINPERSON = "DELIVER IN PERSON"
    AIR = "AIR"
    AIRREG = "AIRREG"
    lsel = (
        (
            ((lineitem.L_QUANTITY <= 36) & (lineitem.L_QUANTITY >= 26))
            | ((lineitem.L_QUANTITY <= 25) & (lineitem.L_QUANTITY >= 15))
            | ((lineitem.L_QUANTITY <= 14) & (lineitem.L_QUANTITY >= 4))
        )
        & (lineitem.L_SHIPINSTRUCT == DELIVERINPERSON)
        & ((lineitem.L_SHIPMODE == AIR) | (lineitem.L_SHIPMODE == AIRREG))
    )
    psel = (part.P_SIZE >= 1) & (
        (
            (part.P_SIZE <= 5)
            & (part.P_BRAND == Brand31)
            & (
                (part.P_CONTAINER == SMBOX)
                | (part.P_CONTAINER == SMCASE)
                | (part.P_CONTAINER == SMPACK)
                | (part.P_CONTAINER == SMPKG)
            )
        )
        | (
            (part.P_SIZE <= 10)
            & (part.P_BRAND == Brand43)
            & (
                (part.P_CONTAINER == MEDBAG)
                | (part.P_CONTAINER == MEDBOX)
                | (part.P_CONTAINER == MEDPACK)
                | (part.P_CONTAINER == MEDPKG)
            )
        )
        | (
            (part.P_SIZE <= 15)
            & (part.P_BRAND == Brand43)
            & (
                (part.P_CONTAINER == LGBOX)
                | (part.P_CONTAINER == LGCASE)
                | (part.P_CONTAINER == LGPACK)
                | (part.P_CONTAINER == LGPKG)
            )
        )
    )
    flineitem = lineitem[lsel]
    fpart = part[psel]
    jn = flineitem.merge(fpart, left_on="L_PARTKEY", right_on="P_PARTKEY")
    jnsel = (
        (jn.P_BRAND == Brand31)
        & (
            (jn.P_CONTAINER == SMBOX)
            | (jn.P_CONTAINER == SMCASE)
            | (jn.P_CONTAINER == SMPACK)
            | (jn.P_CONTAINER == SMPKG)
        )
        & (jn.L_QUANTITY >= 4)
        & (jn.L_QUANTITY <= 14)
        & (jn.P_SIZE <= 5)
        | (jn.P_BRAND == Brand43)
        & (
            (jn.P_CONTAINER == MEDBAG)
            | (jn.P_CONTAINER == MEDBOX)
            | (jn.P_CONTAINER == MEDPACK)
            | (jn.P_CONTAINER == MEDPKG)
        )
        & (jn.L_QUANTITY >= 15)
        & (jn.L_QUANTITY <= 25)
        & (jn.P_SIZE <= 10)
        | (jn.P_BRAND == Brand43)
        & (
            (jn.P_CONTAINER == LGBOX)
            | (jn.P_CONTAINER == LGCASE)
            | (jn.P_CONTAINER == LGPACK)
            | (jn.P_CONTAINER == LGPKG)
        )
        & (jn.L_QUANTITY >= 26)
        & (jn.L_QUANTITY <= 36)
        & (jn.P_SIZE <= 15)
    )
    jn = jn[jnsel]
    total = (jn.L_EXTENDEDPRICE * (1.0 - jn.L_DISCOUNT)).sum()
    print(total)
    print("Q19 Execution time (s): ", time.time() - t1)



def q20(lineitem, part, nation, partsupp, supplier):
    t1 = time.time()
    date1 = pd.Timestamp("1996-01-01")
    date2 = pd.Timestamp("1997-01-01")
    psel = part.P_NAME.str.startswith("azure")
    nsel = nation.N_NAME == "JORDAN"
    lsel = (lineitem.L_SHIPDATE >= date1) & (lineitem.L_SHIPDATE < date2)
    fpart = part[psel]
    fnation = nation[nsel]
    flineitem = lineitem[lsel]
    jn1 = fpart.merge(partsupp, left_on="P_PARTKEY", right_on="PS_PARTKEY")
    jn2 = jn1.merge(
        flineitem,
        left_on=["PS_PARTKEY", "PS_SUPPKEY"],
        right_on=["L_PARTKEY", "L_SUPPKEY"],
    )
    gb = jn2.groupby(["PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY"], as_index=False)[
        "L_QUANTITY"
    ].sum()
    gbsel = gb.PS_AVAILQTY > (0.5 * gb.L_QUANTITY)
    fgb = gb[gbsel]
    jn3 = fgb.merge(supplier, left_on="PS_SUPPKEY", right_on="S_SUPPKEY")
    jn4 = fnation.merge(jn3, left_on="N_NATIONKEY", right_on="S_NATIONKEY")
    jn4 = jn4.loc[:, ["S_NAME", "S_ADDRESS"]]
    total = jn4.sort_values("S_NAME").drop_duplicates()
    print(total)
    print("Q20 Execution time (s): ", time.time() - t1)



def q21(lineitem, orders, supplier, nation):
    t1 = time.time()
    lineitem_filtered = lineitem.loc[:, ["L_ORDERKEY", "L_SUPPKEY", "L_RECEIPTDATE", "L_COMMITDATE"]]

    # Exists
    lineitem_orderkeys = lineitem_filtered.loc[:, ["L_ORDERKEY", "L_SUPPKEY"]].groupby("L_ORDERKEY", as_index=False)["L_SUPPKEY"].nunique()
    lineitem_orderkeys.columns = ["L_ORDERKEY", "nunique_col"]
    lineitem_orderkeys = lineitem_orderkeys[lineitem_orderkeys["nunique_col"] > 1]
    lineitem_orderkeys = lineitem_orderkeys.loc[:, ["L_ORDERKEY"]]

    # Filter
    lineitem_filtered = lineitem_filtered[lineitem_filtered["L_RECEIPTDATE"] > lineitem_filtered["L_COMMITDATE"]]
    lineitem_filtered = lineitem_filtered.loc[:, ["L_ORDERKEY", "L_SUPPKEY"]]

    # Merge Filter + Exists
    lineitem_filtered = lineitem_filtered.merge(lineitem_orderkeys, on="L_ORDERKEY", how="inner")

    # Not Exists: Check the exists condition isn't still satisfied on the output.
    lineitem_orderkeys = lineitem_filtered.groupby("L_ORDERKEY", as_index=False)["L_SUPPKEY"].nunique()
    lineitem_orderkeys.columns = ["L_ORDERKEY", "nunique_col"]
    lineitem_orderkeys = lineitem_orderkeys[lineitem_orderkeys["nunique_col"] == 1]
    lineitem_orderkeys = lineitem_orderkeys.loc[:, ["L_ORDERKEY"]]

    # Merge Filter + Not Exists
    lineitem_filtered = lineitem_filtered.merge(lineitem_orderkeys, on="L_ORDERKEY", how="inner")

    orders_filtered = orders.loc[:, ["O_ORDERSTATUS", "O_ORDERKEY"]]
    orders_filtered = orders_filtered[orders_filtered["O_ORDERSTATUS"] == "F"]
    orders_filtered = orders_filtered.loc[:, ["O_ORDERKEY"]]
    total = lineitem_filtered.merge(orders_filtered, left_on="L_ORDERKEY", right_on="O_ORDERKEY", how="inner")
    total = total.loc[:, ["L_SUPPKEY"]]

    supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NATIONKEY", "S_NAME"]]
    total = total.merge(supplier_filtered, left_on="L_SUPPKEY", right_on="S_SUPPKEY", how="inner")
    total = total.loc[:, ["S_NATIONKEY", "S_NAME"]]
    nation_filtered = nation.loc[:, ["N_NAME", "N_NATIONKEY"]]
    nation_filtered = nation_filtered[nation_filtered["N_NAME"] == "SAUDI ARABIA"]
    total = total.merge(nation_filtered, left_on="S_NATIONKEY", right_on="N_NATIONKEY", how="inner")
    total = total.loc[:, ["S_NAME"]]
    total = total.groupby("S_NAME", as_index=False).size()
    total.columns = ["S_NAME", "NUMWAIT"]
    total = total.sort_values(by=["NUMWAIT","S_NAME",], ascending=[False,True,])
    print(total)
    print("Q21 Execution time (s): ", time.time() - t1)



def q22(customer, orders):
    t1 = time.time()
    customer_filtered = customer.loc[:, ["C_ACCTBAL", "C_CUSTKEY"]]
    customer_filtered["CNTRYCODE"] = customer["C_PHONE"].str.slice(0, 2)
    customer_filtered = customer_filtered[(customer["C_ACCTBAL"] > 0.00) & customer_filtered["CNTRYCODE"].isin(["13", "31", "23", "29", "30", "18", "17"])]
    avg_value = customer_filtered["C_ACCTBAL"].mean()
    customer_filtered = customer_filtered[customer_filtered["C_ACCTBAL"] > avg_value]
    # Select only the keys that don't match by performing a left join and only selecting columns with an na value
    orders_filtered = orders.loc[:, ["O_CUSTKEY"]].drop_duplicates()
    customer_keys = customer_filtered.loc[:, ["C_CUSTKEY"]].drop_duplicates()
    customer_selected = customer_keys.merge(orders_filtered, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')
    customer_selected= customer_selected[customer_selected["O_CUSTKEY"].isna()]
    customer_selected = customer_selected.loc[:, ["C_CUSTKEY"]]
    customer_selected = customer_selected.merge(customer_filtered, on="C_CUSTKEY", how="inner")
    customer_selected = customer_selected.loc[:, ["CNTRYCODE", "C_ACCTBAL"]]
    agg1 = customer_selected.groupby(["CNTRYCODE"], as_index = False).size()
    agg1.columns = ["CNTRYCODE", "NUMCUST"]
    agg2 = customer_selected.groupby(["CNTRYCODE"], as_index = False).agg(TOTACCTBAL=pd.NamedAgg(column="C_ACCTBAL", aggfunc="sum"))
    total = agg1.merge(agg2, on="CNTRYCODE", how="inner")
    total = total.sort_values(by=["CNTRYCODE",], ascending=[True,])
    print(total)
    print("Q22 Execution time (s): ", time.time() - t1)


def main():
    path = "/mnt/data/mdata"
    run_queries(path)

if __name__ == "__main__":
    main()
