package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class SimpleSparkJob {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SimpleSparkJob")
                .setMaster("yarn");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession.builder()
                .appName("DataFrameExample")
                .master("yarn")
                .getOrCreate();

        // Создание коллекции объектов
        List<Row> data = Arrays.asList(
                RowFactory.create(1, "Alice"),
                RowFactory.create(2, "Bob"),
                RowFactory.create(3, "Sasha"),
                RowFactory.create(4, "Alex"),
                RowFactory.create(5, "Artem")
        );


        // Определение схемы
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("name", DataTypes.StringType);



        JavaRDD<Row> rdd = sc.parallelize(data);
        Dataset<Row> df = spark.createDataFrame(rdd, schema);

        Dataset<Row> productsDF = spark.read()
                .option("multiline", "true")
                .json("hdfs://namenode:9000/spark/products.json");

        productsDF.createOrReplaceTempView("products_table");


        Dataset<Row> resultDF1 = spark.sql(
                "SELECT o.order_id, o.user_id, p.product_name, o.order_date " +
                        "FROM orders_table o " +
                        "JOIN products_table p " +
                        "ON o.product_id = p.product_id"
        );



        Dataset<Row> ordersDF = spark.read()
                .option("header", "true")
                .csv("hdfs://namenode:9000/spark/orders.csv");



        ordersDF.createOrReplaceTempView("orders_table");

        df.createOrReplaceTempView("user_table");

        Dataset<Row> resultDF = spark.sql("SELECT * FROM user_table").orderBy("name");
        resultDF.coalesce(1).write().mode("overwrite").csv("hdfs://namenode:9000/out_spark_sql/output/0");

        resultDF1.coalesce(1).write().mode("overwrite").parquet("hdfs://namenode:9000/out_spark_sql/output/1");

        Dataset<Row> resultDF2 = spark.sql(
                "SELECT o.order_id, o.user_id, p.product_name, o.order_date " +
                        "FROM orders_table o " +
                        "JOIN products_table p " + "ON o.product_id = p.product_id"
        );
        resultDF2.coalesce(1).write().mode("overwrite").parquet("hdfs://namenode:9000/out_spark_sql/output/2");

        Dataset<Row> totalAmountByUser = spark.sql(
                "SELECT o.user_id, SUM(p.price) AS total_amount " +
                        "FROM orders_table o " +
                        "JOIN products_table p " +
                        "ON o.product_id = p.product_id " +
                        "GROUP BY o.user_id"
        );
        totalAmountByUser.coalesce(1).write().mode("overwrite").parquet("hdfs://namenode:9000/out_spark_sql/output/3");
        Dataset<Row> highSalesProducts = spark.sql(
                "SELECT p.product_name, SUM(p.price) AS total_sales " +
                        "FROM products_table p " +
                        "JOIN orders_table o " +
                        "ON p.product_id = o.product_id " +
                        "GROUP BY p.product_name " +
                        "HAVING total_sales > 500"
        );
        highSalesProducts.coalesce(1).write().mode("overwrite").parquet("hdfs://namenode:9000/out_spark_sql/output/4");
        Dataset<Row> expensiveProducts = spark.sql(
                "SELECT * " +
                        "FROM products_table " +
                        "WHERE price > 400"
        );
        expensiveProducts.coalesce(1).write().mode("overwrite").parquet("hdfs://namenode:9000/out_spark_sql/output/5");

        Dataset<Row> avgPriceByCategory = spark.sql(
                "SELECT category, AVG(price) AS average_price " +
                        "FROM products_table " +
                        "GROUP BY category"
        );
        avgPriceByCategory.coalesce(1).write().mode("overwrite").parquet("hdfs://namenode:9000/out_spark_sql/output/6");

        // Выполняем агрегацию для подсчета общей суммы заказов по пользователям
        Dataset<Row> orderProductSummary = spark.sql(
                "SELECT o.user_id, COUNT(o.order_id) AS total_orders, " +
                        "SUM(p.price) AS total_spent, " +
                        "MAX(p.price) AS max_spent " +
                        "FROM orders_table o " +
                        "JOIN products_table p ON o.product_id = p.product_id " +
                        "GROUP BY o.user_id"
        );
        orderProductSummary.coalesce(1).write().mode("overwrite").parquet("hdfs://namenode:9000/out_spark_sql/output/7");


        spark.stop();
    }

}
