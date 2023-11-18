package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.sum;


public class SparkKafka {
    private static final String KAFKA_BROKERS = "kafka-test:9092";
    private static final String KAFKA_TOPIC = "PRODUCT";
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("SparkKafka")
                .config("spark.sql.streaming.checkpointLocation", "file:///spark/spark1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        StructType productSchema = new StructType()
                .add("productId", DataTypes.StringType)
                .add("productName", DataTypes.StringType)
                .add("description", DataTypes.StringType)
                .add("price", DataTypes.LongType)
                .add("brand", DataTypes.StringType);

        Dataset<Row> kafkaData = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                .option("subscribe", KAFKA_TOPIC)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), productSchema).as("data"))
                .select("data.*");

        Dataset<Row> result = kafkaData.groupBy("brand")
                .agg(sum("price").as("totalPrice"), count("*").as("count"));


        result.selectExpr("to_json(struct(*)) as value")
                .writeStream()
                .outputMode("complete")
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                .option("topic", "SPARK_OUT")
                .start()
                .awaitTermination();

    }
}

