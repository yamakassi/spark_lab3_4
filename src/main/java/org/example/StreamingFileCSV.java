package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.stddev;
import static org.apache.spark.sql.functions.window;

public class StreamingFileCSV {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("csvStreaming")
                .getOrCreate();
        Logger.getRootLogger().setLevel(Level.ERROR);


        StructType jsonSchema = new StructType()
                .add("currency", "string")
                .add("price", "double")
                .add("timestamp", "timestamp");

        String localFolderPath = "file:///inputCSV";

        Dataset<Row> csvDF = spark
                .readStream()
                .option("sep", ",")
                .schema(jsonSchema)
                .option("header", "true")
                .csv(localFolderPath)
                .withWatermark("timestamp", "2 minutes");
        
        Dataset<Row> aggregatedData = csvDF
                .groupBy(window(col("timestamp"), "1 minute", "30 seconds"), col("currency"))
                .agg(

                        avg("price").alias("AveragePrice"),
                        stddev("price").alias("PriceStdDev")
                );


        StreamingQuery query = aggregatedData.writeStream()
                .outputMode("append")
                .format("parquet")
                .option("checkpointLocation", "file:///spark")
                .option("path", "file:///out")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        }
    }
}
