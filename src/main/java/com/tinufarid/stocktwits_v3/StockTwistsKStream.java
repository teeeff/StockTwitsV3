package com.tinufarid.stocktwits_v3;

import jdk.nashorn.internal.parser.JSONParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;


import java.util.Properties;

public class StockTwistsKStream {


    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String,String> kStream = streamsBuilder.stream(AppConfigs.topicName);
        //kStream.foreach((k,v) -> System.out.println(v));




        KStream<String,String> kStream1 = kStream.filter((k,v) -> new JSONObject(v).getString("sentiment").equals("Bullish"));
        //kStream1.foreach((k,v) -> System.out.println(v));


        KStream<String,String> kStream2 = kStream.filter((k,v) -> new JSONObject(v).getString("sentiment").equals("Bearish"));
        kStream2.foreach((k,v) -> System.out.println(  new JSONObject(v).getString("body")));

        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }


}
