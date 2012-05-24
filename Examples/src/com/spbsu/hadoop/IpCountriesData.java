package com.spbsu.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

/**
 * User: svasilinets
 * Date: 28.04.12
 * Time: 13:23
 */
public class IpCountriesData {

    private static final String UNKNOWN_COUNTRY = "unknown";

    private static class Info {
        private final String country;
        private final long beginIp;
        private final long endIp;

        private Info(String country, long beginIp, long endIp) {
            this.country = country;
            this.beginIp = beginIp;
            this.endIp = endIp;
        }
    }


    private static long convertIpToNum(String ip) {
        try {
            StringTokenizer tokenizer = new StringTokenizer(ip, ".");
            long a = Long.parseLong(tokenizer.nextToken());
            long b = Long.parseLong(tokenizer.nextToken());
            long c = Long.parseLong(tokenizer.nextToken());
            long d = Long.parseLong(tokenizer.nextToken());

            return 16777216L * a + 65536L * b + 256L * c + d;
        } catch (Exception e){
//            e.printStackTrace();
            return 0L;
        }
    }

    private static String normalize(String s) {
        return s.substring(1, s.length() - 1);
    }


    private TreeMap<Long, Info> db = new TreeMap<Long, Info>();


    public IpCountriesData(String path) throws IOException {
        File cities = new File(path);
        BufferedReader reader = new BufferedReader(new FileReader(cities));
        String s = reader.readLine();
        while (s != null) {
            StringTokenizer tokenizer = new StringTokenizer(s, ",");
            tokenizer.nextToken();
            tokenizer.nextToken();

            String beginNum = tokenizer.nextToken();
            String endNum = tokenizer.nextToken();
            long begin = Long.parseLong(normalize(beginNum));
            long end = Long.parseLong(normalize(endNum));

            tokenizer.nextToken();

            String country = normalize(tokenizer.nextToken());
            db.put(begin, new Info(country, begin, end));
            s = reader.readLine();
        }
        reader.close();
    }

    public String getCountryByIp(String ip) {
        long num = convertIpToNum(ip);
        Map.Entry<Long, Info> entry = db.floorEntry(num);
        if (entry == null || entry.getValue().endIp < num) {
            return UNKNOWN_COUNTRY;
        }

        return entry.getValue().country;
    }

}
