package com.controller.service;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class PartitioningService {

    public String getWorkerForKey(String key, List<String> workers) {
        if (workers == null || workers.isEmpty()) throw new IllegalStateException("No active workers");
        int hash = Math.abs(key.hashCode());
        int idx = hash % workers.size();
        return workers.get(idx);
    }

    public List<String> getReplicaWorkers(String primaryWorkerUrl, List<String> workers) {
        List<String> replicas = new ArrayList<>();
        if (workers == null || workers.size() <= 1) return replicas;
        int i = workers.indexOf(primaryWorkerUrl);
        if (i == -1) return replicas;
        for (int j = 1; j <= 2 && j < workers.size(); j++) {
            replicas.add(workers.get((i + j) % workers.size()));
        }
        return replicas;
    }
}
