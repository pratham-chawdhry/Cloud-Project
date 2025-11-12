package com.controller.service;

import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class PartitioningService {

    /**
     * Determines which worker should handle a given key using hash-based partitioning.
     * Uses consistent hashing approach with modulo operation.
     *
     * @param key The key to partition
     * @param workers List of active worker URLs
     * @return The worker URL that should handle this key
     */
    public String getWorkerForKey(String key, List<String> workers) {
        if (workers == null || workers.isEmpty()) {
            throw new IllegalStateException("No active workers available");
        }
        
        // Hash the key and use modulo to determine which worker handles it
        int hash = key.hashCode();
        int workerIndex = Math.abs(hash) % workers.size();
        return workers.get(workerIndex);
    }

    /**
     * Gets the replica workers for a given primary worker.
     * Returns the next 2 workers in the ring for replication.
     *
     * @param primaryWorkerUrl The primary worker URL
     * @param workers List of all active worker URLs
     * @return List of replica worker URLs (up to 2)
     */
    public List<String> getReplicaWorkers(String primaryWorkerUrl, List<String> workers) {
        List<String> replicas = new java.util.ArrayList<>();
        if (workers == null || workers.size() <= 1) {
            return replicas;
        }

        int primaryIndex = workers.indexOf(primaryWorkerUrl);
        if (primaryIndex == -1) {
            return replicas;
        }

        // Get next 2 workers in the ring for replication
        for (int i = 1; i <= 2 && i < workers.size(); i++) {
            int replicaIndex = (primaryIndex + i) % workers.size();
            replicas.add(workers.get(replicaIndex));
        }

        return replicas;
    }
}
