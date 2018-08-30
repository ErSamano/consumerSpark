package com.esparaquia;

//import org.apache.spark.SparkConf;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;

public class Main {
    public static void main(String[] a) {

        //Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("IoTSensor");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        //Create a DStream that will connect to hostname:port, localhost:9092
        //This <<lines>> DStream represents the stream of data that will be received from the data server.
        // Each record in this stream is a line of text
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost",9999);

        //Split each line into words
        //<<flatMap>> is a DStream operation that creates a new DStream by generating multiple new records
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        //Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordsCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

        //Print the first ten elements of each RDD generated in this DStream to the console
        wordsCounts.print();

        //Start the computation
        jssc.start();

        //Wait for the computation to terminate
        jssc.awaitTermination();

    }
}