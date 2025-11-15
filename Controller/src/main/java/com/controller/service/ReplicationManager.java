package com.controller.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class ReplicationManager {

    private final RestTemplate restTemplate;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private final WorkerManager workerManager;
    private final PartitioningService partitioningService;

    @Autowired
    public ReplicationManager(WorkerManager workerManager, PartitioningService partitioningService) {
        this.workerManager = workerManager;
        this.partitioningService = partitioningService;
        this.restTemplate = createRestTemplateWithTimeouts();
    }

    private RestTemplate createRestTemplateWithTimeouts() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(2000); // 2s
        factory.setReadTimeout(5000);    // 5s
        return new RestTemplate(factory);
    }

    /**
     * Handles worker failure by re-replicating missing keys from remaining replicas.
     * Runs in a background thread to avoid blocking caller.
     */
    public void handleWorkerFailure(String failedWorkerUrl) {
        System.out.println("Handling failure of worker: " + failedWorkerUrl);

        executor.submit(() -> {
            try {
                Thread.sleep(1000); // brief delay

                List<String> activeWorkers = workerManager.getActiveWorkers();
                // Exclude the failed worker if still present
                activeWorkers.removeIf(url -> url.equals(failedWorkerUrl));

                if (activeWorkers.isEmpty()) {
                    System.err.println("No active workers available for re-replication");
                    return;
                }

                Map<String, String> allData = new HashMap<>();
                Set<String> processedKeys = new HashSet<>();

                // Collect data from all active workers
                for (String workerUrl : activeWorkers) {
                    try {
                        Map<String, Object> response = restTemplate.getForObject(workerUrl + "/status", Map.class);
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

                // Re-replicate keys to ensure each key has at least required replicas
                for (Map.Entry<String, String> entry : allData.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();

                    if (processedKeys.contains(key)) continue;
                    processedKeys.add(key);

                    String primaryWorker = partitioningService.getWorkerForKey(key, activeWorkers);
                    List<String> replicas = partitioningService.getReplicaWorkers(primaryWorker, activeWorkers);

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
     * Ensures the key exists on primary and required number of replicas.
     * Returns true if replication succeeded to the minimum required count.
     */
    private boolean ensureReplication(String key, String value, String primaryWorker, List<String> replicas) {
        int successCount = 0;

        // Ensure primary has the data
        if (replicateToWorker(key, value, primaryWorker)) {
            successCount++;
        }

        // Ensure up to 2 replicas (adjust per your replication factor)
        int replicaCount = 0;
        for (String replica : replicas) {
            if (replicaCount >= 2) break;
            if (replicateToWorker(key, value, replica)) {
                successCount++;
                replicaCount++;
            }
        }

        if (successCount < 2) {
            System.err.println("Warning: Key " + key + " has only " + successCount + " replicas (need at least 2)");
            return false;
        }

        return true;
    }

    /**
     * Replicates a single key/value to a worker via HTTP POST.
     */
    private boolean replicateToWorker(String key, String value, String workerUrl) {
        if (workerUrl == null) return false;
        if (!workerManager.isWorkerHealthy(workerUrl)) {
            return false;
        }

        try {
            Map<String, String> request = new HashMap<>();
            request.put("key", key);
            request.put("value", value);

            restTemplate.postForEntity(workerUrl + "/replicate", request, Object.class);
            return true;
        } catch (RestClientException e) {
            System.err.println("Failed to replicate key " + key + " to worker " + workerUrl + ": " + e.getMessage());
            return false;
        } catch (Exception e) {
            System.err.println("Unexpected error replicating key " + key + " to " + workerUrl + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * Handles worker recovery by ensuring the recovered worker receives keys it should own.
     */
    public void handleWorkerRecovery(String recoveredWorkerUrl) {
        System.out.println("Handling recovery of worker: " + recoveredWorkerUrl);

        executor.submit(() -> {
            try {
                List<String> activeWorkers = workerManager.getActiveWorkers();

                for (String workerUrl : activeWorkers) {
                    if (workerUrl.equals(recoveredWorkerUrl)) continue;

                    try {
                        Map<String, Object> response = restTemplate.getForObject(workerUrl + "/status", Map.class);
                        if (response != null && response.containsKey("payload")) {
                            Object payloadObj = response.get("payload");
                            if (payloadObj instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, String> data = (Map<String, String>) payloadObj;

                                if (data != null && !data.isEmpty()) {
                                    for (Map.Entry<String, String> entry : data.entrySet()) {
                                        String key = entry.getKey();
                                        String value = entry.getValue();

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
                e.printStackTrace();
            }
        });
    }
}
