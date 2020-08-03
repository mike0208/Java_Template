package com.example.activemq;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

public class PropertiesCache {
    private final Properties configProp;

    public PropertiesCache() {
        this.configProp = new Properties();
        InputStream in = this.getClass().getClassLoader().getResourceAsStream("application.properties");
        System.out.println("Read all properties from file");

        try {
            this.configProp.load(in);
        } catch (IOException var3) {
            var3.printStackTrace();
        }

    }

    public static PropertiesCache getInstance() {
        return PropertiesCache.LazyHolder.INSTANCE;
    }

    public String getProperty(String key) {
        return this.configProp.getProperty(key);
    }

    public Set<String> getAllPropertyNames() {
        return this.configProp.stringPropertyNames();
    }

    public boolean containsKey(String key) {
        return this.configProp.containsKey(key);
    }

    public void setProperty(String key, String value) {
        this.configProp.setProperty(key, value);
    }

    private static class LazyHolder {
        private static final PropertiesCache INSTANCE = new PropertiesCache();

        private LazyHolder() {
        }
    }
}
