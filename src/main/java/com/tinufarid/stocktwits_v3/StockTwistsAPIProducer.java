package com.tinufarid.stocktwits_v3;


import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONArray;
import org.json.JSONObject;

public class StockTwistsAPIProducer {


    public static JSONArray ApiProducer() throws UnirestException {

        Unirest.setTimeouts(0, 0);
        HttpResponse<String> response = Unirest.get("https://api.stocktwits.com/api/2/streams/trending.json")
                .asString();

        JSONObject obj = new JSONObject(response.getBody());

        JSONArray messages = obj.getJSONArray("messages");

        //System.out.println(messages);

        return messages;

    }

}
