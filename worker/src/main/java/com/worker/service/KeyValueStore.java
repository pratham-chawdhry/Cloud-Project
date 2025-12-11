package com.worker.service;

import com.worker.model.KeyValue;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KeyValueStore {

    // Store the entire KeyValue object
    private final ConcurrentHashMap<String, KeyValue> store = new ConcurrentHashMap<>();

    public void put(KeyValue keyValue) {
        store.put(keyValue.getKey(), keyValue);
    }

    public KeyValue get(String key) {
        return store.get(key);
    }

    public Map<String, KeyValue> getAll() {
        return new HashMap<>(store);
    }

    public void remove(String key) {
        store.remove(key);
    }

    public boolean contains(String key) {
        return store.containsKey(key);
    }
}
