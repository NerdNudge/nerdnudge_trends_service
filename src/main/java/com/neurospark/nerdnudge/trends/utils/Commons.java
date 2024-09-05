package com.neurospark.nerdnudge.trends.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

public class Commons {

    public static Properties getProperties(final String configFileName) {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(configFileName)) {
            properties.load(fis);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return properties;
    }

    public static String getDaystamp() {
        LocalDate date = LocalDate.now();
        int dayOfYear = date.getDayOfYear();
        int year = date.getYear() % 100;
        String dayOfYearStr = String.format("%03d", dayOfYear);
        String yearStr = String.format("%02d", year);
        return dayOfYearStr + yearStr;
    }

    public static void housekeepDayJsonObject(JsonObject jsonObject, int retentionEntries) {
        Set<Map.Entry <String , JsonElement>> dailyQuotaKeys = jsonObject.entrySet();
        if(dailyQuotaKeys.size() <= retentionEntries)
            return;

        TreeSet<String> sortedKeys = new TreeSet<>();
        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            sortedKeys.add(entry.getKey());
        }

        while(jsonObject.entrySet().size() > retentionEntries) {
            String oldestKey = sortedKeys.pollFirst();
            if (oldestKey != null) {
                jsonObject.remove(oldestKey);
            }
        }
    }
}
