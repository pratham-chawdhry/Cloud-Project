package com.controller.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class WorkerRegistry {

    private static class WorkerInfo {
        final String url;
        volatile long lastHeartbeat;

        WorkerInfo(String url) {
            this.url = url;
            this.lastHeartbeat = System.currentTimeMillis();
        }
    }

    @Value("${controller.heartbeat.timeout:15}")
    private long heartbeatTimeoutSeconds;

    private final Map<String, WorkerInfo> workers = new ConcurrentHashMap<>();

    public void register(String workerId, String url) {
        workers.put(workerId, new WorkerInfo(url));
    }

    public void updateHeartbeat(String workerId) {
        WorkerInfo info = workers.get(workerId);
        if (info != null) info.lastHeartbeat = System.currentTimeMillis();
    }

    public List<String> getAliveWorkers() {
        long now = System.currentTimeMillis();
        long timeout = heartbeatTimeoutSeconds * 1000L;

        return workers.entrySet().stream()
                .filter(e -> (now - e.getValue().lastHeartbeat) < timeout)
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
    }

    public String getUrl(String workerId) {
        WorkerInfo info = workers.get(workerId);
        return info == null ? null : info.url;
    }

    public Long getLastHeartbeat(String workerId) {
        WorkerInfo info = workers.get(workerId);
        return info == null ? null : info.lastHeartbeat;
    }

    public boolean markDead(String workerId) {
        return workers.remove(workerId) != null;
    }

    public Set<String> getAllWorkers() { return workers.keySet(); }

    public void restoreWorker(String workerId, String url) {
        workers.put(workerId, new WorkerInfo(url)); // heartbeat = now
    }
}
