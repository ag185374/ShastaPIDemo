package com.example.shastadataflow;

import java.nio.charset.StandardCharsets;

public class Main {
    public static void main(String[] args) {
        String json = "{\n" +
                "    \"version\": 1,\n" +
                "    \"itemCode\": \"1234\",\n" +
                "    \"UPC\": \"40231000\",\n" +
                "    \"documentName\": \"\",\n" +
                "    \"documentId\": \"\",\n" +
                "    \"documentType\": \"free count\",\n" +
                "    \"documentStatus\": \"active\",\n" +
                "    \"count\": 12,\n" +
                "    \"effectiveDate\": \"2020-09-17T12:13:21.755Z\",\n" +
                "    \"endDate\": \"2020-06-11T16:01:12.976Z\",\n" +
                "    \"basePrice\": 3.009\n" +
                "}";
        byte[] bytes = json.getBytes();
        String s = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("json string: " + s);
    }
}
