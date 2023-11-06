package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class SparkStreaming {

    public static void main(String[] args) throws InterruptedException, TimeoutException {
        SparkSession spark = SparkSession.builder()
                .appName("socket")
                .getOrCreate();

        StructType jsonSchema = new StructType()
                .add("name", "string")
                .add("spent", "integer")
                .add("city", "string")
                .add("timestamp", "Timestamp");


        Dataset<Row> inputDF = spark
                .readStream()
                .format("socket")
                .option("host", "0.0.0.0")
                .option("port", 9999)
                .load()
                .selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), jsonSchema).as("data"))
                .select("data.*");
        inputDF.printSchema();

        Dataset<Row> aggregatedDF = inputDF
                .withWatermark("timestamp", "1 minute")
                .groupBy(window(col("timestamp"), "1 minute", "30 seconds"), col("city"))
                .agg(avg("spent").as("avg_value"));

        StreamingQuery query = aggregatedDF.coalesce(1)
                .writeStream()
                .outputMode("append")
                .format("parquet")
                .option("checkpointLocation", "file:///spark/spark1")
                .option("path", "file:///out/out1")
                .trigger(Trigger.ProcessingTime("2 minutes"))
                .start();

        try {
            query.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            query.stop();
        }
    }
}
