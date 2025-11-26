package com.controller.service;

import com.controller.model.WorkerNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
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

    @Value("${controller.config.retry.delay:500}")
    private long configRetryDelayMs;

    @Value("${controller.config.retry.attempts:3}")
    private int configRetryAttempts;

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
        factory.setConnectTimeout(2000);
        factory.setReadTimeout(5000);
        this.restTemplate = new RestTemplate(factory);
    }

    /**
     * Load workers either from disk or from config.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {

        StatePersistenceService.ControllerState recoveredState = statePersistenceService.loadState();

        if (recoveredState != null && !recoveredState.getWorkers().isEmpty()) {
            workers.putAll(recoveredState.getWorkers());
            workerUrls.clear();
            workerUrls.addAll(recoveredState.getWorkerUrls());

            System.out.println("Recovered " + workers.size() + " workers from disk.");
            for (WorkerNode w : workers.values()) {
                System.out.println(" - " + w.getUrl() + " active=" + w.isActive());
            }
        } else {
            String[] urls = workersConfig.split(",");
            for (int i = 0; i < urls.length; i++) {
                String url = urls[i].trim();
                WorkerNode worker = new WorkerNode(url, i);
                worker.setActive(false); // will be set true by HeartbeatMonitor
                workers.put(url, worker);
                workerUrls.add(url);
            }
            saveState();
        }

        // configure replicas once workers start responding
        new Thread(() -> {
            try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            configureWorkersWithRetries();
        }, "initial-replica-config").start();
    }

    /**
     * Configure all workers with replica lists.
     */
    private void configureWorkersWithRetries() {

        List<String> activeWorkers = getActiveWorkers();
        if (activeWorkers.isEmpty()) {
            System.err.println("No active workers available for replica config.");
            return;
        }

        for (String workerUrl : activeWorkers) {

            List<String> replicaCandidates = partitioningService.getReplicaWorkers(workerUrl, activeWorkers);

            boolean success = false;
            int attempts = 0;

            while (!success && attempts < configRetryAttempts) {
                attempts++;
                try {
                    configureWorkerReplicas(workerUrl, replicaCandidates);
                    success = true;
                } catch (Exception e) {
                    System.err.println("Failed configuring replicas for " + workerUrl +
                            " attempt=" + attempts + " error=" + e.getMessage());
                    try { Thread.sleep(configRetryDelayMs); } catch (InterruptedException ignored) {}
                }
            }
        }
    }

    /**
     * POST /config/replicas
     */
    private void configureWorkerReplicas(String workerUrl, List<String> replicaUrls) {

        WorkerNode node = workers.get(workerUrl);
        if (node == null) throw new IllegalStateException("Unknown worker " + workerUrl);
        if (!node.isActive()) throw new IllegalStateException("Worker inactive: " + workerUrl);

        String sync = replicaUrls.size() > 0 ? replicaUrls.get(0) : null;
        String async = replicaUrls.size() > 1 ? replicaUrls.get(1) : null;

        Map<String, String> payload = new HashMap<>();
        payload.put("syncReplica", sync);
        payload.put("asyncReplica", async);

        restTemplate.postForEntity(workerUrl + "/config/replicas", payload, Object.class);
        node.setReplicaUrls(replicaUrls);
        saveStateAsync();

        System.out.println("Configured worker " + workerUrl + " -> " + replicaUrls);
    }

    public boolean isWorkerActive(String workerUrl) {
        WorkerNode n = workers.get(workerUrl);
        return n != null && n.isActive();
    }

    public void markWorkerAsFailed(String workerUrl) {
        WorkerNode node = workers.get(workerUrl);
        if (node == null) return;

        if (node.isActive()) {
            node.setActive(false);
            System.out.println("[DOWN] " + workerUrl);

            try {
                replicationManager.handleWorkerFailure(workerUrl);
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }

            new Thread(this::configureWorkersWithRetries).start();
            saveStateAsync();
        }
    }

    public void markWorkerAsRecovered(String workerUrl) {
        WorkerNode node = workers.get(workerUrl);
        if (node == null) return;

        if (!node.isActive()) {
            node.setActive(true);
            System.out.println("[UP] " + workerUrl);

            try {
                replicationManager.handleWorkerRecovery(workerUrl);
            } catch (Exception e) {
                System.err.println("Recovery error: " + e.getMessage());
            }

            new Thread(this::configureWorkersWithRetries).start();
            saveStateAsync();
        }
    }

    public List<String> getAllWorkers() {
        return new ArrayList<>(workerUrls);
    }

    public Collection<WorkerNode> getAllWorkerNodes() {
        return workers.values();
    }

    public List<String> getActiveWorkers() {
        return workerUrls.stream()
                .filter(this::isWorkerActive)
                .collect(Collectors.toList());
    }

    /**
     * Returns the primary worker responsible for a given key.
     * Uses only currently active workers.
     */
    public String getWorkerForKey(String key) {
        List<String> active = getActiveWorkers();
        if (active == null || active.isEmpty()) return null;

        return partitioningService.getWorkerForKey(key, active);
    }


    private void saveState() {
        statePersistenceService.saveState(workers, workerUrls);
    }

    private void saveStateAsync() {
        new Thread(() -> {
            try { Thread.sleep(100); } catch (Exception ignored) {}
            saveState();
        }).start();
    }
}
