package com.esparaquia;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class Main {

    static Map<String, Object> kafkaParams = new HashMap<>();

    public static void main(String[] args) throws InterruptedException, StreamingQueryException {


        // Create a local StreamingContext with thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkConsumer");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "0");
        kafkaParams.put("auto.offset.reset", "earliest"); // from-beginning?
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("LP300CATs18");

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaPairDStream<String, String> jPairDStream =  stream.mapToPair(
                new PairFunction<ConsumerRecord<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(ConsumerRecord<String, String> record) throws Exception {
                        return new Tuple2<>(record.key(), record.value());
                    }
                });

        JavaPairDStream<String, String> jWindowDStream  = jPairDStream.reduceByKeyAndWindow((i1, i2) -> i1 + i2, Durations.seconds(30), Durations.seconds(10));
        System.out.println("Windowed Data in last 30 seconds every 10 seconds");
        jWindowDStream.print();
        System.out.println("Windowed Data Ends");


        //To print all the consumed data
        jPairDStream.foreachRDD(jPairRDD -> {
        jPairRDD.foreach(rdd -> {
        Random rand = new Random();
        int  n = rand.nextInt(5000) + 1;
                System.out.println("key="+n+" VALUE="+rdd._2());
            });
        });

        jssc.start();
        jssc.awaitTermination();


    }//File end
}