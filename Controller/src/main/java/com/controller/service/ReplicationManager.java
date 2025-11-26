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
        factory.setConnectTimeout(2000);
        factory.setReadTimeout(5000);
        return new RestTemplate(factory);
    }


    /* ============================================================
       WORKER FAILURE HANDLING – RECOVERY OF LOST REPLICAS
    ============================================================= */
    public void handleWorkerFailure(String failedWorkerUrl) {

        System.out.println("Handling failure of worker: " + failedWorkerUrl);

        executor.submit(() -> {
            try {
                Thread.sleep(1000);

                List<String> activeWorkers = workerManager.getActiveWorkers();
                activeWorkers.remove(failedWorkerUrl);

                if (activeWorkers.isEmpty()) {
                    System.err.println("No active workers available for re-replication");
                    return;
                }

                Map<String, String> combinedData = new HashMap<>();

                for (String workerUrl : activeWorkers) {
                    Map<String, String> data = fetchWorkerKV(workerUrl);
                    if (data != null) combinedData.putAll(data);
                }

                for (Map.Entry<String, String> entry : combinedData.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();

                    String primary = partitioningService.getWorkerForKey(key, activeWorkers);
                    List<String> replicas = partitioningService.getReplicaWorkers(primary, activeWorkers);

                    ensureReplication(key, value, primary, replicas);
                }

                System.out.println("Re-replication after failure completed.");

            } catch (Exception e) {
                System.err.println("Error in handleWorkerFailure: " + e.getMessage());
            }
        });
    }


    /* ============================================================
       ENSURE REPLICATION FACTOR FOR A KEY
    ============================================================= */
    private boolean ensureReplication(String key, String value, String primary, List<String> replicas) {
        int successCount = 0;

        // Primary
        if (replicateToWorker(key, value, primary)) successCount++;

        int replicaCount = 0;
        for (String replica : replicas) {
            if (replicaCount >= 2) break;
            if (replicateToWorker(key, value, replica)) {
                successCount++;
                replicaCount++;
            }
        }

        if (successCount < 2) {
            System.err.println("Warning: Key " + key + " has only " + successCount + " replicas (needs 2)");
            return false;
        }

        return true;
    }


    /* ============================================================
       SEND KEY/VAL TO TARGET WORKER
    ============================================================= */
    private boolean replicateToWorker(String key, String value, String workerUrl) {
        if (workerUrl == null) return false;
        if (!workerManager.isWorkerActive(workerUrl)) return false;

        try {
            Map<String, String> req = Map.of("key", key, "value", value);

            restTemplate.postForEntity(workerUrl + "/replicate", req, Object.class);
            return true;

        } catch (RestClientException e) {
            System.err.println("Failed replicate " + key + " -> " + workerUrl + " : " + e.getMessage());
            return false;
        }
    }


    /* ============================================================
       WORKER RECOVERY HANDLING
    ============================================================= */
    public void handleWorkerRecovery(String recoveredWorkerUrl) {

        System.out.println("Handling recovery of worker: " + recoveredWorkerUrl);

        executor.submit(() -> {
            try {
                List<String> activeWorkers = workerManager.getActiveWorkers();

                for (String workerUrl : activeWorkers) {
                    if (workerUrl.equals(recoveredWorkerUrl)) continue;

                    Map<String, String> data = fetchWorkerKV(workerUrl);
                    if (data == null) continue;

                    for (Map.Entry<String, String> entry : data.entrySet()) {
                        String key = entry.getKey();
                        String value = entry.getValue();

                        String primary = partitioningService.getWorkerForKey(key, activeWorkers);
                        List<String> replicas = partitioningService.getReplicaWorkers(primary, activeWorkers);

                        // If recovered worker should hold this key
                        if (recoveredWorkerUrl.equals(primary) ||
                                replicas.contains(recoveredWorkerUrl)) {

                            replicateToWorker(key, value, recoveredWorkerUrl);
                        }
                    }
                }

                System.out.println("Recovery replication completed for " + recoveredWorkerUrl);

            } catch (Exception e) {
                System.err.println("Error in handleWorkerRecovery: " + e.getMessage());
            }
        });
    }


    /* ============================================================
       FETCH /status FROM A WORKER SAFELY
    ============================================================= */
    private Map<String, String> fetchWorkerKV(String workerUrl) {
        if (!workerManager.isWorkerActive(workerUrl)) return null;

        try {
            Map<String, Object> resp =
                    restTemplate.getForObject(workerUrl + "/status", Map.class);

            if (resp == null) return null;
            if (!resp.containsKey("payload")) return null;

            Object payloadObj = resp.get("payload");
            if (payloadObj instanceof Map<?, ?> payload) {
                @SuppressWarnings("unchecked")
                Map<String, String> casted = (Map<String, String>) payload;
                return casted;
            }

            return null;

        } catch (Exception e) {
            System.err.println("Error fetching status from " + workerUrl + ": " + e.getMessage());
            return null;
        }
    }
}
