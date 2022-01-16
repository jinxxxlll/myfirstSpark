package com.jinx;

import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class ComputeLog {

    @Test
    public void readFiles() throws IOException {

        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\jinxxx\\Desktop\\model.txt"),StandardCharsets.UTF_8));
        BufferedReader in2 = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\jinxxx\\Desktop\\1.txt"), StandardCharsets.UTF_8));
        HashMap<String, ArrayList<String>> model = new HashMap<>();
        ArrayList<String> log = new ArrayList<>();
        HashSet<String> keys = new HashSet<>();
        String str = null;
        while ((str = in.readLine()) != null) {
            String[] strings = str.split("\t");
            ArrayList<String> temp = new ArrayList<>();
            temp.add(strings[2]);
            temp.add(strings[1]);
            model.put(strings[0],temp);
        }

        while ((str = in2.readLine()) != null) {
            String[] split = str.split(" - ");
            log.addAll(Arrays.asList(split));
        }

        for (String s : log) {
            for (String key : model.keySet()) {
                if(s.contains(key)){
                    keys.add(key);
                }
            }
        }
        for (String key : keys) {
            System.out.println(key+"\t"+model.get(key).get(0)+"\t"+model.get(key).get(1));
        }

        in.close();
        in2.close();

    }
}
