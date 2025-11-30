package com.controller.service;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.*;

@Service
public class ReplicationManager {

    private final WorkerManager manager;
    private final PartitioningService partition;
    private final RestTemplate rest = new RestTemplate();
    private final ExecutorService exec = Executors.newCachedThreadPool();

    public ReplicationManager(@Lazy WorkerManager manager,
                              PartitioningService partition) {
        this.manager = manager;
        this.partition = partition;
    }

    public void handleFailure(String failedUrl) {
        exec.submit(() -> {
            List<String> active = new ArrayList<>(manager.getActiveWorkers());
            active.removeIf(u -> u == null || u.equals(failedUrl));
            if (active.isEmpty()) return;

            Map<String, String> data = readData(active);
            replicateAll(data, active);
        });
    }

    public void recoverWorker(String url) {
        exec.submit(() -> {
            List<String> active = manager.getActiveWorkers();
            if (!active.contains(url)) return;

            Map<String, String> data = readData(active);

            for (var e : data.entrySet()) {
                String key = e.getKey();
                String value = e.getValue();

                String primary = partition.getWorkerForKey(key, active);
                List<String> replicas = partition.getReplicaWorkers(primary, active);

                if (url.equals(primary) || replicas.contains(url)) {
                    replicateOne(key, value, url);
                }
            }
        });
    }

    public void recoverAll() {
        exec.submit(() -> {
            List<String> active = manager.getActiveWorkers();
            if (active.isEmpty()) return;

            Map<String, String> data = readData(active);
            replicateAll(data, active);
        });
    }

    private Map<String, String> readData(List<String> active) {
        Map<String, String> all = new HashMap<>();
        for (String w : active) {
            try {
                Map resp = rest.getForObject(w + "/status", Map.class);
                if (resp != null && resp.get("payload") instanceof Map p) {
                    all.putAll((Map<String, String>) p);
                }
            } catch (Exception ignored) {}
        }
        return all;
    }

    private void replicateAll(Map<String, String> allData, List<String> active) {
        for (var e : allData.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();

            String primary = partition.getWorkerForKey(key, active);
            List<String> replicas = partition.getReplicaWorkers(primary, active);

            replicateOne(key, value, primary);
            for (String r : replicas) replicateOne(key, value, r);
        }
    }

    private void replicateOne(String key, String value, String url) {
        try {
            rest.postForEntity(url + "/replicate",
                    Map.of("key", key, "value", value), Void.class);
        } catch (Exception ignored) {}
    }
}
