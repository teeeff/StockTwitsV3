package com.tinufarid.stocktwits_v3;


import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

public class SampleApiCall {

    public static void main(String[] args) throws IOException {




        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        Request request = new Request.Builder()
                .url("https://api.stocktwits.com/api/2/streams/trending.json")
                .method("GET", null)
                .build();
        Response response = client.newCall(request).execute();

        System.out.println(response.body().string());




    }


}
