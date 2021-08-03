package com.tinufarid.stocktwits_v3;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static com.tinufarid.stocktwits_v3.ApplicationConfig.*;

public class StockTwitsKafkaConsumer {


    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Logger logger= LoggerFactory.getLogger(StockTwitsKafkaConsumer.class.getName());
        String grp_id="third_app";
        //Creating consumer properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,grp_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //creating consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String,String>(properties);
        //Subscribing
        consumer.subscribe(Arrays.asList(topic));

        //Class.forName("com.mysql.jdbc.Driver");
        Class.forName("com.mysql.cj.jdbc.Driver");

        Connection conn= DriverManager.getConnection(
                "jdbc:mysql://ms.itversity.com:3306/retail_db?" +
                        "useUnicode=true&useJDBCCompliantTimezoneShift=true" +
                        "&useLegacyDatetimeCode=false" +
                        "&serverTimezone=UTC","retail_user","itversity");


        String SQL = "INSERT INTO retail_export.test_table2"
                + "(id, created_at, body, sentiment, followers, like_count, following, ideas, symbols, username)"
                + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";


        PreparedStatement prst = conn.prepareStatement(SQL);

        //polling
        while(true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record: records){
                //logger.info("Key: "+ record.key() + ", Value:" +record.value());



                try {
                    JSONObject jsonObject = new JSONObject(record.value());


                    prst.setString(1, jsonObject.getString("id"));
                    prst.setString(2, jsonObject.getString("created_at"));
                    prst.setString(3, jsonObject.getString("body"));
                    prst.setString(4, jsonObject.getString("sentiment"));
                    prst.setString(5, jsonObject.getString("followers"));
                    prst.setString(6, jsonObject.getString("like_count"));
                    prst.setString(7, jsonObject.getString("following"));
                    prst.setString(8, jsonObject.getString("ideas"));
                    prst.setString(9, jsonObject.getJSONArray("symbols").toString());
                    prst.setString(10, jsonObject.getString("username"));
                    //prst.addBatch();


                    try {
                        prst.executeUpdate();
                    }
                    catch(Exception e){

                        System.out.println("ignoring duplicate");
                    }

                }catch (JSONException err){
                    logger.error(err.toString());
                }



                //logger.info("Partition:" + record.partition()+",Offset:"+record.offset());
            }


        }

        //prst.close();
        //conn.close();
    }


}
