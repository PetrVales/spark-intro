package com.gooddata.horizon;

import org.apache.spark.sql.DataFrame;
import static org.apache.spark.sql.functions.*;

public class TpchExamples extends WithSparkContext {

    public static void main(String[] args) {
        setupContext();

        final DataFrame customer = readTblAsDataFrame("/1g/customer.tbl");
        final DataFrame lineItem = readTblAsDataFrame("/1g/lineitem.tbl");
        final DataFrame nation = readTblAsDataFrame("/1g/nation.tbl");
        final DataFrame orders = readTblAsDataFrame("/1g/orders.tbl");
        final DataFrame part = readTblAsDataFrame("/1g/part.tbl");
        final DataFrame partSupp = readTblAsDataFrame("/1g/partsupp.tbl");
        final DataFrame region = readTblAsDataFrame("/1g/region.tbl");
        final DataFrame supplier = readTblAsDataFrame("/1g/supplier.tbl");

//        customer.show();
//        lineItem.show();
//        nation.show();
//        orders.show();
//        part.show();
//        partSupp.show();
//        region.show();
//        supplier.show();

        long start = System.currentTimeMillis();
        threeSql(customer, orders, lineItem)
                .write().format("com.databricks.spark.csv").option("header", "true").save("output");
        System.out.println((System.currentTimeMillis() - start) / 1000 + "s");
    }

    static DataFrame three(DataFrame customer, DataFrame orders, DataFrame lineItem) {
        return customer.join(orders, col("c_custkey").equalTo(col("o_custkey")))
                .join(lineItem, col("l_orderkey").equalTo(col("o_orderkey")))
                .where(
                        col("c_mktsegment").equalTo("BUILDING")
                    .and(
                        col("o_orderdate").lt("1994-02-04")
                    .and(
                        col("l_shipdate").gt("1993-03-05")
                    ))
                )
                .selectExpr("*", "l_extendedprice * (1 - l_discount) AS vzorecek")
                .groupBy(col("l_orderkey"), col("o_orderdate"), col("o_shippriority"))
                .agg(col("l_orderkey"), sum("vzorecek").as("revenue"), col("o_orderdate"), col("o_shippriority"))
                .orderBy(col("revenue").desc(), col("o_orderdate"))
                .select(col("l_orderkey"), col("revenue"), col("o_orderdate"), col("o_shippriority"));
    }

    static DataFrame threeSql(DataFrame customer, DataFrame orders, DataFrame lineItem) {
        customer.registerTempTable("customer");
        orders.registerTempTable("orders");
        lineItem.registerTempTable("lineitem");

        return sqlContext.sql(
                "select\n" +
                    "l_orderkey,\n" +
                    "sum(l_extendedprice * (1 - l_discount)) as revenue,\n" +
                    "o_orderdate,\n" +
                    "o_shippriority\n" +
                "from\n" +
                    "customer,\n" +
                    "orders,\n" +
                    "lineitem\n" +
                "where\n" +
                    "c_mktsegment = 'BUILDING'\n" +
                    "and c_custkey = o_custkey\n" +
                    "and l_orderkey = o_orderkey\n" +
                    "and o_orderdate < '1994-02-04'\n" +
                    "and l_shipdate > '1993-03-05'\n" +
                "group by\n" +
                    "l_orderkey,\n" +
                    "o_orderdate,\n" +
                    "o_shippriority\n" +
                "order by\n" +
                    "revenue desc,\n" +
                    "o_orderdate"
        );
    }

    static DataFrame thirteenSql(DataFrame customer, DataFrame orders) {
        customer.registerTempTable("customer");
        orders.registerTempTable("orders");

        return sqlContext.sql(
                "select\n" +
                    "c_count,\n" +
                    "count(*) as custdist\n" +
                "from\n" +
                    "(\n" +
                        "select\n" +
                            "c_custkey,\n" +
                            "count(o_orderkey) as c_count\n" +
                        "from\n" +
                            "customer left outer join orders on\n" +
                                "c_custkey = o_custkey\n" +
                                "and o_comment not like '%a%t%'\n" +
                        "group by\n" +
                            "c_custkey\n" +
                    ") as c_orders\n" +
                "group by\n" +
                    "c_count\n" +
                "order by\n" +
                    "custdist desc,\n" +
                    "c_count desc");
    }

    static DataFrame sixteenSql(DataFrame part, DataFrame partSupp, DataFrame supplier) {
        part.registerTempTable("part");
        partSupp.registerTempTable("partsupp");
        supplier.registerTempTable("supplier");

        return sqlContext.sql(
                "select\n" +
                    "p_brand,\n" +
                    "p_type,\n" +
                    "p_size,\n" +
                    "count(distinct ps_suppkey) as supplier_cnt\n" +
                "from\n" +
                    "partsupp,\n" +
                    "part\n" +
                "where\n" +
                    "p_partkey = ps_partkey\n" +
                    "and p_brand <> 'Brand#13'\n" +
                    "and p_type not like 'LARGE%'\n" +
                    "and p_size in (1, 14, 45, 46, 47, 48, 12, 15)\n" +
                    "and ps_suppkey not in (\n" +
                        "select\n" +
                            "s_suppkey\n" +
                        "from\n" +
                            "supplier\n" +
                        "where\n" +
                            "s_comment like '%Customer%Complaints%'\n" +
                    ")\n" +
                "group by\n" +
                    "p_brand,\n" +
                    "p_type,\n" +
                    "p_size\n" +
                "order by\n" +
                    "supplier_cnt desc,\n" +
                    "p_brand,\n" +
                    "p_type,\n" +
                    "p_size"
        );
    }

    static DataFrame nineSql(DataFrame part, DataFrame partSupp, DataFrame supplier, DataFrame orders, DataFrame lineItem, DataFrame nation) {
        part.registerTempTable("part");
        partSupp.registerTempTable("partsupp");
        supplier.registerTempTable("supplier");
        orders.registerTempTable("orders");
        lineItem.registerTempTable("lineitem");
        nation.registerTempTable("nation");

        return sqlContext.sql(
                "select\n" +
                    "nation,\n" +
                    "o_year,\n" +
                    "sum(amount) as sum_profit\n" +
                "from\n" +
                    "(\n" +
                        "select\n" +
                            "n_name as nation,\n" +
                            "extract(year from o_orderdate) as o_year,\n" +
                            "l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n" +
                        "from\n" +
                            "part,\n" +
                            "supplier,\n" +
                            "lineitem,\n" +
                            "partsupp,\n" +
                            "orders,\n" +
                            "nation\n" +
                        "where\n" +
                            "s_suppkey = l_suppkey\n" +
                            "and ps_suppkey = l_suppkey\n" +
                            "and ps_partkey = l_partkey\n" +
                            "and p_partkey = l_partkey\n" +
                            "and o_orderkey = l_orderkey\n" +
                            "and s_nationkey = n_nationkey\n" +
                            "and p_name like '%:1%'\n" +
                    ") as profit\n" +
                "group by\n" +
                    "nation,\n" +
                    "o_year\n" +
                "order by\n" +
                    "nation,\n" +
                    "o_year desc");
    }

}
