package org.example;

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

public class StreamingFileCSV {

    public static void main(String[] args) throws TimeoutException {
        SparkSession spark = SparkSession.builder()
                .appName("csvStreaming")
                .master("local[2]")
                .getOrCreate();

        StructType jsonSchema = new StructType()
                .add("currency", "string")
                .add("price", "double");

        String localFolderPath = "file:///inputCSV";

        Dataset<Row> csvDF = spark
                .readStream()
                .option("sep", ",")
                .schema(jsonSchema)
                .option("header", "true")
                .csv(localFolderPath);

        // Группировка и агрегация
        Dataset<Row> aggregatedData = csvDF
                .groupBy("currency")
                .agg(
                        avg("price").alias("AveragePrice"),
                        stddev("price").alias("PriceStdDev")
                );

        StreamingQuery query = aggregatedData.writeStream()
                .outputMode("update")
                .format("console")
                .trigger(Trigger.ProcessingTime("1 minute"))
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        }
    }
}
