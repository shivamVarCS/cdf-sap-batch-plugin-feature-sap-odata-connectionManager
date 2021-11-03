package com.google.cloud.datafusion.plugin.sap.odata.source.util;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileNotFoundException;
import java.io.FileReader;

public class SampleFileReader {

    public static void main(String[] args ){

        JSONParser parser = new JSONParser();
        FileReader reader = null;
        try {
            reader = new FileReader("/Users/varshneys/Downloads/schema.json");
            JSONObject object = (JSONObject) parser.parse(reader);
            System.out.print(String.valueOf(object));
        } catch (FileNotFoundException fileNotFoundException) {

            System.out.print("Print" + fileNotFoundException);
        } catch (Exception e) {

        }
    }
}

