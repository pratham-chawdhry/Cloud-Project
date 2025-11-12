package com.controller.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class ReplicationManager {

    private final RestTemplate restTemplate = new RestTemplate();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Autowired
    private WorkerManager workerManager;

    @Autowired
    private PartitioningService partitioningService;

    /**
     * Handles worker failure by re-replicating data from remaining replicas.
     * Ensures all keys that were on the failed worker are re-replicated to maintain 3 replicas.
     */
    public void handleWorkerFailure(String failedWorkerUrl) {
        System.out.println("Handling failure of worker: " + failedWorkerUrl);
        
        executor.submit(() -> {
            try {
                // Wait a bit to ensure all active workers are updated
                Thread.sleep(1000);
                
                // Get all active workers (excluding the failed one)
                List<String> activeWorkers = workerManager.getActiveWorkers();
                
                if (activeWorkers.isEmpty()) {
                    System.err.println("No active workers available for re-replication");
                    return;
                }
                
                // Collect all key-value pairs from all active workers
                Map<String, String> allData = new HashMap<>();
                Set<String> processedKeys = new HashSet<>();
                
                // First, collect all data from active workers
                for (String workerUrl : activeWorkers) {
                    try {
                        Map<String, Object> response = restTemplate.getForObject(
                            workerUrl + "/status",
                            Map.class
                        );
                        
                        // The response from /status is ApiResponse<Map<String, String>>
                        // Structure: {"status": "success", "code": 200, "errorMessage": null, "payload": {"key1": "value1", ...}}
                        if (response != null && response.containsKey("payload")) {
                            Object payloadObj = response.get("payload");
                            if (payloadObj instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, String> data = (Map<String, String>) payloadObj;
                                if (data != null && !data.isEmpty()) {
                                    allData.putAll(data);
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error reading data from worker " + workerUrl + ": " + e.getMessage());
                    }
                }
                
                // For each key-value pair, ensure it's replicated to the correct workers
                for (Map.Entry<String, String> entry : allData.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    
                    if (processedKeys.contains(key)) {
                        continue; // Already processed this key
                    }
                    processedKeys.add(key);
                    
                    // Determine which workers should have this key based on current active workers
                    String primaryWorker = partitioningService.getWorkerForKey(key, activeWorkers);
                    List<String> replicas = partitioningService.getReplicaWorkers(primaryWorker, activeWorkers);
                    
                    // Ensure the key exists on primary and at least 2 replicas
                    ensureReplication(key, value, primaryWorker, replicas);
                }
                
                System.out.println("Re-replication completed for " + processedKeys.size() + " keys");
            } catch (Exception e) {
                System.err.println("Error in handleWorkerFailure: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    /**
     * Ensures a key-value pair is replicated to the primary worker and its replicas.
     * Returns true if at least 2 replicas (including primary) have the data.
     */
    private boolean ensureReplication(String key, String value, String primaryWorker, List<String> replicas) {
        int successCount = 0;
        
        // Ensure primary has the data
        if (replicateToWorker(key, value, primaryWorker)) {
            successCount++;
        }
        
        // Ensure replicas have the data (we need at least 2 total replicas)
        int replicaCount = 0;
        for (String replica : replicas) {
            if (replicaCount >= 2) break; // We only need 2 replicas total (primary + 1 replica)
            if (replicateToWorker(key, value, replica)) {
                successCount++;
                replicaCount++;
            }
        }
        
        // We need at least 2 replicas (including primary) for put to succeed
        if (successCount < 2) {
            System.err.println("Warning: Key " + key + " has only " + successCount + " replicas (need at least 2)");
            return false;
        }
        
        return true;
    }

    /**
     * Replicates a key-value pair to a worker.
     */
    private boolean replicateToWorker(String key, String value, String workerUrl) {
        if (!workerManager.isWorkerHealthy(workerUrl)) {
            return false;
        }
        
        try {
            Map<String, String> request = new HashMap<>();
            request.put("key", key);
            request.put("value", value);
            
            restTemplate.postForEntity(
                workerUrl + "/replicate",
                request,
                Object.class
            );
            return true;
        } catch (RestClientException e) {
            System.err.println("Failed to replicate key " + key + " to worker " + workerUrl + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * Handles worker recovery by ensuring it has all the data it should have.
     */
    public void handleWorkerRecovery(String recoveredWorkerUrl) {
        System.out.println("Handling recovery of worker: " + recoveredWorkerUrl);
        
        executor.submit(() -> {
            try {
                List<String> activeWorkers = workerManager.getActiveWorkers();
                
                // Get all data from other workers and replicate to recovered worker if needed
                for (String workerUrl : activeWorkers) {
                    if (workerUrl.equals(recoveredWorkerUrl)) {
                        continue;
                    }
                    
                    try {
                        Map<String, Object> response = restTemplate.getForObject(
                            workerUrl + "/status",
                            Map.class
                        );
                        
                        // The response from /status is ApiResponse<Map<String, String>>
                        // Structure: {"status": "success", "code": 200, "errorMessage": null, "payload": {"key1": "value1", ...}}
                        if (response != null && response.containsKey("payload")) {
                            Object payloadObj = response.get("payload");
                            if (payloadObj instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, String> data = (Map<String, String>) payloadObj;
                                
                                if (data != null && !data.isEmpty()) {
                                    for (Map.Entry<String, String> entry : data.entrySet()) {
                                        String key = entry.getKey();
                                        String value = entry.getValue();
                                        
                                        // Check if recovered worker should have this key
                                        String primaryWorker = partitioningService.getWorkerForKey(key, activeWorkers);
                                        List<String> replicas = partitioningService.getReplicaWorkers(primaryWorker, activeWorkers);
                                        
                                        if (recoveredWorkerUrl.equals(primaryWorker) || replicas.contains(recoveredWorkerUrl)) {
                                            replicateToWorker(key, value, recoveredWorkerUrl);
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error during worker recovery from " + workerUrl + ": " + e.getMessage());
                    }
                }
            } catch (Exception e) {
                System.err.println("Error in handleWorkerRecovery: " + e.getMessage());
            }
        });
    }
}
