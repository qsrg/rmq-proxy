package com.mq.proxy.core.storage;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public class StorageAdapterManager {
    private final ConcurrentHashMap<String, StorageAdapter> adapterMap = new ConcurrentHashMap<>();
    private StorageAdapter defaultAdapter;
    private final ConcurrentHashMap<String, StorageAdapter> topicRouteMap = new ConcurrentHashMap<>();

    public void registerAdapter(String name, StorageAdapter adapter, boolean isDefault) {
        adapterMap.put(name, adapter);
        if (isDefault) {
            defaultAdapter = adapter;
        }
    }

    public StorageAdapter getAdapter(String name) {
        return adapterMap.get(name);
    }

    public StorageAdapter getAdapterByTopic(String topic) {
        if (topic != null) {
            StorageAdapter adapter = topicRouteMap.get(topic);
            if (adapter != null) {
                return adapter;
            }
        }
        return defaultAdapter;
    }

    public void routeTopic(String topic, String adapterName) {
        StorageAdapter adapter = adapterMap.get(adapterName);
        if (adapter != null) {
            topicRouteMap.put(topic, adapter);
        }
    }

    public void initializeAll(Map<String, StorageConfig> configs) throws Exception {
        ServiceLoader<StorageAdapter> loader = ServiceLoader.load(StorageAdapter.class);
        boolean firstRegistered = false;
        for (StorageAdapter adapter : loader) {
            for (Map.Entry<String, StorageConfig> entry : configs.entrySet()) {
                StorageConfig config = entry.getValue();
                if (adapter.getAdapterName().equals(config.getAdapterType())) {
                    adapter.initialize(config);
                    boolean isDefault = !firstRegistered;
                    registerAdapter(entry.getKey(), adapter, isDefault);
                    firstRegistered = true;
                    break;
                }
            }
        }
    }

    public void shutdownAll() {
        for (StorageAdapter adapter : adapterMap.values()) {
            adapter.shutdown();
        }
        adapterMap.clear();
        topicRouteMap.clear();
        defaultAdapter = null;
    }

    public boolean healthCheckAll() {
        if (adapterMap.isEmpty()) {
            return false;
        }
        for (StorageAdapter adapter : adapterMap.values()) {
            if (!adapter.healthCheck()) {
                return false;
            }
        }
        return true;
    }
}
