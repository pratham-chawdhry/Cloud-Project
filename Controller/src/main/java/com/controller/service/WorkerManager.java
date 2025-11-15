package com.controller.service;

import com.controller.model.WorkerNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@Service
public class WorkerManager {

    private final Map<String, WorkerNode> workers = new ConcurrentHashMap<>();
    private final List<String> workerUrls = new CopyOnWriteArrayList<>();

    private RestTemplate restTemplate;

    private final PartitioningService partitioningService;
    private final ReplicationManager replicationManager;
    private final StatePersistenceService statePersistenceService;

    @Value("${controller.workers:http://localhost:8081,http://localhost:8082,http://localhost:8083,http://localhost:8084}")
    private String workersConfig;

    @Value("${controller.heartbeat.timeout:10}")
    private long heartbeatTimeoutSeconds;

    @Value("${controller.state.save.interval:30000}")
    private long stateSaveIntervalMs;

    @Autowired
    public WorkerManager(PartitioningService partitioningService,
                         @Lazy ReplicationManager replicationManager,
                         StatePersistenceService statePersistenceService) {
        this.partitioningService = partitioningService;
        this.replicationManager = replicationManager;
        this.statePersistenceService = statePersistenceService;
        initRestTemplate();
    }

    private void initRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(2000); // 2s connect timeout
        factory.setReadTimeout(5000);    // 5s read timeout
        this.restTemplate = new RestTemplate(factory);
    }

    /**
     * Populate initial worker list. This does NOT perform remote calls.
     * Called on ApplicationReadyEvent to avoid blocking startup with network calls.
     * First tries to recover state from disk, otherwise initializes from config.
     */
    private void initializeWorkers() {
        // Try to recover state from disk first
        StatePersistenceService.ControllerState recoveredState = statePersistenceService.loadState();

        if (recoveredState != null && !recoveredState.getWorkers().isEmpty()) {
            // Recover from saved state
            System.out.println("Recovering controller state from disk...");
            workers.putAll(recoveredState.getWorkers());
            workerUrls.clear();
            workerUrls.addAll(recoveredState.getWorkerUrls());

            System.out.println("Recovered " + workers.size() + " workers from saved state:");
            for (WorkerNode worker : workers.values()) {
                System.out.println("  - " + worker.getUrl() + " (ID: " + worker.getWorkerId() +
                    ", Active: " + worker.isActive() + ", Replicas: " + worker.getReplicaUrls() + ")");
            }
        } else {
            // Initialize from config (first time or no saved state)
            System.out.println("Initializing workers from configuration...");
            String[] urls = workersConfig.split(",");
            for (int i = 0; i < urls.length; i++) {
                String url = urls[i].trim();
                WorkerNode worker = new WorkerNode(url, i);
                workers.put(url, worker);
                workerUrls.add(url);
                System.out.println("Registered worker: " + url + " with ID: " + i);
            }
            // Save initial state
            saveState();
        }
    }

    /**
     * Called after Spring Boot is fully started.
     * Runs the worker configuration asynchronously with retries.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        initializeWorkers();

        Thread configThread = new Thread(() -> {
            try {
                // small delay to let workers (if started separately) have time to come up
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {}

            configureWorkersWithRetries();
        }, "worker-config-thread");

        configThread.setDaemon(true);
        configThread.start();
    }

    /**
     * Configure replica lists for all known workers with retries, but do not abort application startup on failure.
     */
    private void configureWorkersWithRetries() {
        List<String> allWorkers = getAllWorkers();
        for (String workerUrl : allWorkers) {
            List<String> replicas = partitioningService.getReplicaWorkers(workerUrl, allWorkers);

            boolean success = false;
            int attempts = 0;
            while (!success && attempts < 3) {
                attempts++;
                try {
                    configureWorkerReplicas(workerUrl, replicas);
                    success = true;
                } catch (Exception e) {
                    System.err.println("Attempt " + attempts + " failed to configure " + workerUrl + ": " + e.getMessage());
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ignored) {}
                }
            }
            if (!success) {
                System.err.println("Giving up configuring replicas for " + workerUrl + " after 3 attempts");
            }
        }
    }

    /**
     * Configure a worker's replica list via HTTP POST. Throws exception on failure so callers can retry.
     */
    private void configureWorkerReplicas(String workerUrl, List<String> replicaUrls) {
        WorkerNode worker = workers.get(workerUrl);
        if (worker == null) {
            throw new IllegalStateException("Worker not found: " + workerUrl);
        }

        if (!isWorkerHealthy(workerUrl)) {
            throw new IllegalStateException("Worker not healthy: " + workerUrl);
        }

        // Will throw RestClientException on failure, which propagates to caller for retry logic.
        restTemplate.postForEntity(workerUrl + "/config/replicas", replicaUrls, Object.class);
        worker.setReplicaUrls(replicaUrls);
        System.out.println("Configured worker " + workerUrl + " with replicas: " + replicaUrls);

        // Save state after configuring replicas
        saveStateAsync();
    }

    /**
     * Returns the primary worker for a given key (delegates to partitioningService).
     */
    public String getWorkerForKey(String key) {
        List<String> activeWorkers = getActiveWorkers();
        return partitioningService.getWorkerForKey(key, activeWorkers);
    }

    /**
     * Returns currently active workers (based on health checks).
     */
    public List<String> getActiveWorkers() {
        return workerUrls.stream()
                .filter(this::isWorkerHealthy)
                .collect(Collectors.toList());
    }

    /**
     * Returns all configured workers (active + inactive).
     */
    public List<String> getAllWorkers() {
        return new ArrayList<>(workerUrls);
    }

    /**
     * Checks if a worker is healthy based on last heartbeat and active flag.
     */
    public boolean isWorkerHealthy(String workerUrl) {
        WorkerNode worker = workers.get(workerUrl);
        if (worker == null) return false;

        if (worker.isHeartbeatStale(heartbeatTimeoutSeconds)) {
            worker.setActive(false);
            return false;
        }

        return worker.isActive();
    }

    /**
     * Update heartbeat for a worker. If worker transitions from inactive -> active,
     * reconfigure workers and trigger recovery logic in replication manager.
     */
    public void updateHeartbeat(String workerUrl) {
        WorkerNode worker = workers.get(workerUrl);
        if (worker != null) {
            boolean wasActive = worker.isActive();
            worker.updateHeartbeat();
            if (!wasActive) {
                worker.setActive(true);
                System.out.println("Worker " + workerUrl + " is back online");
                // Reconfigure worker replica lists asynchronously
                new Thread(this::configureWorkersWithRetries, "reconfigure-after-recovery").start();

                // Trigger re-replication for keys that belong to this worker
                try {
                    replicationManager.handleWorkerRecovery(workerUrl);
                } catch (Exception e) {
                    System.err.println("Error calling replicationManager.handleWorkerRecovery: " + e.getMessage());
                }
            }
            // Save state after heartbeat update (async to avoid blocking)
            saveStateAsync();
        } else {
            System.err.println("updateHeartbeat: unknown worker " + workerUrl);
        }
    }

    /**
     * Mark a worker as failed and trigger re-replication.
     */
    public void markWorkerAsFailed(String workerUrl) {
        WorkerNode worker = workers.get(workerUrl);
        if (worker != null && worker.isActive()) {
            worker.setActive(false);
            System.out.println("Worker " + workerUrl + " marked as failed");

            try {
                replicationManager.handleWorkerFailure(workerUrl);
            } catch (Exception e) {
                System.err.println("Error calling replicationManager.handleWorkerFailure: " + e.getMessage());
            }

            // Reconfigure remaining workers asynchronously
            new Thread(this::configureWorkersWithRetries, "reconfigure-after-failure").start();

            // Save state after marking worker as failed
            saveStateAsync();
        }
    }

    public WorkerNode getWorker(String workerUrl) {
        return workers.get(workerUrl);
    }

    public Collection<WorkerNode> getAllWorkerNodes() {
        return workers.values();
    }

    /**
     * Returns a mapping of keys to worker URLs. Currently returns an empty map placeholder
     * — implement per your client requirements when needed.
     */
    public Map<String, String> getKeyToWorkerMapping() {
        return new HashMap<>();
    }

    /**
     * Saves state to disk periodically (scheduled task).
     */
    @Scheduled(fixedDelayString = "${controller.state.save.interval:30000}")
    public void saveStatePeriodically() {
        saveState();
    }

    /**
     * Saves state to disk synchronously.
     */
    private void saveState() {
        statePersistenceService.saveState(workers, workerUrls);
    }

    /**
     * Saves state to disk asynchronously (non-blocking).
     */
    private void saveStateAsync() {
        new Thread(() -> {
            try {
                Thread.sleep(100); // Small delay to batch multiple rapid changes
                saveState();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "save-state-thread").start();
    }
}
