package com.controller.service;

import com.controller.model.WorkerNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import jakarta.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@Service
public class WorkerManager {

    private final Map<String, WorkerNode> workers = new ConcurrentHashMap<>();
    private final List<String> workerUrls = new CopyOnWriteArrayList<>();
    private final RestTemplate restTemplate = new RestTemplate();

    @Autowired
    private PartitioningService partitioningService;

    @Autowired
    private ReplicationManager replicationManager;

    @Value("${controller.workers:http://localhost:8081,http://localhost:8082,http://localhost:8083,http://localhost:8084}")
    private String workersConfig;

    @Value("${controller.heartbeat.timeout:10}")
    private long heartbeatTimeoutSeconds;

    @PostConstruct
    public void initializeWorkers() {
        String[] urls = workersConfig.split(",");
        for (int i = 0; i < urls.length; i++) {
            String url = urls[i].trim();
            WorkerNode worker = new WorkerNode(url, i);
            workers.put(url, worker);
            workerUrls.add(url);
            System.out.println("Registered worker: " + url + " with ID: " + i);
        }
        configureWorkers();
    }

    /**
     * Configures each worker with its replica list.
     */
    public void configureWorkers() {
        List<String> activeWorkers = getActiveWorkers();
        for (String workerUrl : activeWorkers) {
            List<String> replicas = partitioningService.getReplicaWorkers(workerUrl, activeWorkers);
            configureWorkerReplicas(workerUrl, replicas);
        }
    }

    /**
     * Configures a worker's replica list.
     */
    private void configureWorkerReplicas(String workerUrl, List<String> replicaUrls) {
        WorkerNode worker = workers.get(workerUrl);
        if (worker != null && isWorkerHealthy(workerUrl)) {
            try {
                restTemplate.postForEntity(
                    workerUrl + "/config/replicas",
                    replicaUrls,
                    Object.class
                );
                worker.setReplicaUrls(replicaUrls);
                System.out.println("Configured worker " + workerUrl + " with replicas: " + replicaUrls);
            } catch (Exception e) {
                System.err.println("Failed to configure replicas for worker " + workerUrl + ": " + e.getMessage());
            }
        }
    }

    /**
     * Gets the primary worker for a given key.
     */
    public String getWorkerForKey(String key) {
        List<String> activeWorkers = getActiveWorkers();
        return partitioningService.getWorkerForKey(key, activeWorkers);
    }

    /**
     * Gets all active worker URLs.
     */
    public List<String> getActiveWorkers() {
        return workerUrls.stream()
            .filter(this::isWorkerHealthy)
            .collect(Collectors.toList());
    }

    /**
     * Gets all worker URLs (including inactive).
     */
    public List<String> getAllWorkers() {
        return new ArrayList<>(workerUrls);
    }

    /**
     * Checks if a worker is healthy.
     */
    public boolean isWorkerHealthy(String workerUrl) {
        WorkerNode worker = workers.get(workerUrl);
        if (worker == null) return false;
        
        // Check if heartbeat is stale
        if (worker.isHeartbeatStale(heartbeatTimeoutSeconds)) {
            worker.setActive(false);
            return false;
        }
        
        return worker.isActive();
    }

    /**
     * Updates the heartbeat for a worker.
     */
    public void updateHeartbeat(String workerUrl) {
        WorkerNode worker = workers.get(workerUrl);
        if (worker != null) {
            worker.updateHeartbeat();
            // If worker was inactive and just came back, reconfigure
            if (!worker.isActive()) {
                worker.setActive(true);
                System.out.println("Worker " + workerUrl + " is back online");
                configureWorkers();
                // Trigger re-replication for keys that belong to this worker
                replicationManager.handleWorkerRecovery(workerUrl);
            }
        }
    }

    /**
     * Marks a worker as failed and handles re-replication.
     */
    public void markWorkerAsFailed(String workerUrl) {
        WorkerNode worker = workers.get(workerUrl);
        if (worker != null && worker.isActive()) {
            worker.setActive(false);
            System.out.println("Worker " + workerUrl + " marked as failed");
            // Trigger re-replication
            replicationManager.handleWorkerFailure(workerUrl);
            // Reconfigure remaining workers
            configureWorkers();
        }
    }

    /**
     * Gets worker node by URL.
     */
    public WorkerNode getWorker(String workerUrl) {
        return workers.get(workerUrl);
    }

    /**
     * Gets all worker nodes.
     */
    public Collection<WorkerNode> getAllWorkerNodes() {
        return workers.values();
    }

    /**
     * Gets the mapping of keys to workers for client queries.
     */
    public Map<String, String> getKeyToWorkerMapping() {
        Map<String, String> mapping = new HashMap<>();
        List<String> activeWorkers = getActiveWorkers();
        // Generate mapping for sample keys (or clients can query per key)
        // For now, return the active workers list
        return mapping;
    }
}
