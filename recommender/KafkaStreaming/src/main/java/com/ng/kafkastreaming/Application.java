package com.ng.kafkastreaming;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * @Author: Cedaris
 * @Date: 2019/7/2 17:51
 */
public class Application {
    public static void main(String[] args){

        String brokers = "psy831:9092";
        String zookeepers = "psy831:2181";

        // 定义输入和输出的topic
        String from = "log";
        String to = "recommender";

        // 定义kafka streaming的配置
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        StreamsConfig config = new StreamsConfig(settings);

        // 拓扑建构器
        TopologyBuilder builder = new TopologyBuilder();

        // 定义流处理的拓扑结构
        builder.addSource("SOURCE", from)
                .addProcessor("PROCESS",()->new LogProcess(),"SOURCE");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }

}
