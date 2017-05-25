package cn.hackershell.yarn.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by tangshangwen on 17-3-17.
 */
public class Config {
    private static Properties config;

    private Config() {

    }

    public static Map<String,String> create4Env() {
        String configPath = System.getenv("STANDALONE_ON_YARN_HOME")+"/etc/config.properties";
        return create(configPath);
    }
    public static Map<String,String> create(String fileName) {
        Map<String,String> configMap = null;
        try {
            config = getProperties(fileName);
            configMap = new HashMap<>();
            for (Map.Entry<Object, Object> entry: config.entrySet()){
                configMap.put((String)entry.getKey(), (String)entry.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return configMap;
    }

    private static Properties getProperties(String fileName) throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = new FileInputStream(new File(fileName));
        properties.load(inputStream);

        if (inputStream != null) {
            inputStream.close();
        }

        return  properties;
    }
}
