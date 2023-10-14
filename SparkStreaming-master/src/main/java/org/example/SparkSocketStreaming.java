package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkSocketStreaming {
    public static void main(String[] args) {
        SparkConf Scon = new SparkConf()
                .setMaster("local[*]")
                .setAppName("StreaamingOverTCP");
        // Create a local StreamingContext with ALL working thread and batch interval of 2 seconds
        try (JavaStreamingContext JSpark = new JavaStreamingContext(Scon,Duration.apply(10000))) {

            // Create a DStream that will connect to localhost:5252
            JavaReceiverInputDStream<String> dataR = JSpark.socketTextStream("localhost", 5252);

            JavaDStream<String> word = dataR.flatMap(e -> Arrays.asList(e.split(" ")).iterator());

            JavaPairDStream<String, Integer> paires = word.mapToPair(e -> new Tuple2<>(e, 1))
                    .reduceByKey((a, b) -> a + b);

            paires.print();

            JSpark.start();
            try {
                JSpark.awaitTermination();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


    }
}