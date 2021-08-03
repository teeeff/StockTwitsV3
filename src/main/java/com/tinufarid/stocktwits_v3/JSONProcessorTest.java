package com.tinufarid.stocktwits_v3;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONArray;
import org.json.JSONObject;

public class JSONProcessorTest {

    public static void main(String[] args) throws UnirestException {


        JSONArray messages = StockTwistsAPIProducer.ApiProducer();


        for (int i = 0; i < messages.length(); i++) {


            try {

                JSONObject message = messages.getJSONObject(i);


                String messageKey = message.getBigInteger("id").toString();
                String messageValue = message.getString("body");
                String messageCreatedAt = message.getString("created_at");
                String messageSentiment = message.getJSONObject("entities").getJSONObject("sentiment").getString("basic");
                String messageUserName = message.getJSONObject("user").getString("username");
                Integer messageUserFollowers = message.getJSONObject("user").getInt("followers");
                Integer messageUserFollowing = message.getJSONObject("user").getInt("following");
                Integer messageIdeas = message.getJSONObject("user").getInt("ideas");
                Integer messageLikeCount = message.getJSONObject("user").getInt("like_count");
                JSONArray symbols = message.getJSONArray("symbols");

                JSONArray messageSymbols = new JSONArray();

                for (int j = 0; j < symbols.length(); j++) {

                    JSONObject symbol = symbols.getJSONObject(j);


                    messageSymbols.put(symbol.getString("symbol"));


                }


                JSONObject kafkaMessage = new JSONObject();
                kafkaMessage.put("id", messageKey);
                kafkaMessage.put("body", messageValue);
                kafkaMessage.put("created_at", messageCreatedAt);
                kafkaMessage.put("username", messageUserName);
                kafkaMessage.put("followers", messageUserFollowers);
                kafkaMessage.put("following", messageUserFollowing);
                kafkaMessage.put("ideas", messageIdeas);
                kafkaMessage.put("like_count", messageLikeCount);
                kafkaMessage.put("symbols", messageSymbols);
                kafkaMessage.put("sentiment", messageSentiment);

                System.out.println(kafkaMessage);
            }

            catch (Exception e){

                //Just Ignore the record
                ;

            }

        }

    }

}
