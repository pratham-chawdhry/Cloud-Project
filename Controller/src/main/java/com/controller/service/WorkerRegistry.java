package com.controller.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class WorkerRegistry {

    private static class WorkerInfo {
        final String workerId;
        final String url;
        volatile long lastHeartbeat;

        WorkerInfo(String workerId, String url) {
            this.workerId = workerId;
            this.url = url;
            this.lastHeartbeat = System.currentTimeMillis();
        }
    }

    @Value("${controller.heartbeat.timeout:15}")
    private long timeoutSeconds;

    private final Map<String, WorkerInfo> workers = new ConcurrentHashMap<>(); // keyed by workerId
    private final Map<String, String> urlToId = new ConcurrentHashMap<>();     // url -> workerId

    public synchronized void register(String workerId, String url) {
        workers.put(workerId, new WorkerInfo(workerId, url));
        urlToId.put(url, workerId);
        System.out.println("[Registry] register id=" + workerId + " url=" + url);
    }

    public synchronized void restore(String workerId, String url) {
        workers.put(workerId, new WorkerInfo(workerId, url));
        urlToId.put(url, workerId);
        System.out.println("[Registry] restore id=" + workerId + " url=" + url);
    }

    public synchronized void updateHeartbeatById(String workerId) {
        WorkerInfo w = workers.get(workerId);
        if (w != null) w.lastHeartbeat = System.currentTimeMillis();
    }

    public synchronized void updateHeartbeatByUrl(String url) {
        String id = urlToId.get(url);
        if (id != null) updateHeartbeatById(id);
    }

    public Set<String> getAllWorkerIds() { return workers.keySet(); }

    public synchronized List<String> getAliveWorkerUrls() {
        long now = System.currentTimeMillis();
        long timeoutMs = timeoutSeconds * 1000L;
        List<String> out = new ArrayList<>();
        for (var e : workers.entrySet()) {
            if (now - e.getValue().lastHeartbeat < timeoutMs) out.add(e.getValue().url);
        }
        Collections.sort(out);
        return out;
    }

    public synchronized List<String> getAliveWorkerIds() {
        long now = System.currentTimeMillis();
        long timeoutMs = timeoutSeconds * 1000L;
        List<String> out = new ArrayList<>();
        for (var e : workers.entrySet()) {
            if (now - e.getValue().lastHeartbeat < timeoutMs) out.add(e.getKey());
        }
        Collections.sort(out);
        return out;
    }

    public synchronized String getUrl(String workerId) {
        WorkerInfo w = workers.get(workerId);
        return w == null ? null : w.url;
    }

    public synchronized String getWorkerIdByUrl(String url) {
        return urlToId.get(url);
    }

    public synchronized Map<String, Long> snapshotHeartbeats() {
        Map<String, Long> out = new HashMap<>();
        workers.forEach((id, w) -> out.put(id, w.lastHeartbeat));
        return out;
    }

    public synchronized String markDead(String workerId) {
        WorkerInfo removed = workers.remove(workerId);
        if (removed != null) {
            urlToId.remove(removed.url);
            System.out.println("[Registry] markDead id=" + workerId + " url=" + removed.url);
            return removed.url;
        }
        return null;
    }

    public synchronized String removeByUrl(String url) {
        String id = urlToId.remove(url);
        if (id != null) {
            workers.remove(id);
            System.out.println("[Registry] removeByUrl url=" + url + " id=" + id);
        }
        return id;
    }

    public long getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void updateHeartbeat(String workerId) {
        WorkerInfo w = workers.get(workerId);
        if (w != null) w.lastHeartbeat = System.currentTimeMillis();
    }

    public Long getLastHeartbeat(String workerId) {
        WorkerInfo w = workers.get(workerId);
        return (w == null) ? null : w.lastHeartbeat;
    }
}
