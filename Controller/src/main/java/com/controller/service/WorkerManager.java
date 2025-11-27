package com.controller.service;

import com.controller.model.WorkerNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
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
    private final StatePersistenceService statePersistenceService;
    private final ClusterResyncService clusterResyncService;
    private final WorkerRegistry registry;

    @Value("${controller.heartbeat.timeout:10}")
    private long heartbeatTimeoutSeconds;

    @Autowired
    public WorkerManager(PartitioningService partitioningService,
                         StatePersistenceService statePersistenceService,
                         ClusterResyncService clusterResyncService,
                         WorkerRegistry registry) {
        this.partitioningService = partitioningService;
        this.statePersistenceService = statePersistenceService;
        this.clusterResyncService = clusterResyncService;
        this.registry = registry;
        initRestTemplate();
    }

    private void initRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(2000);
        factory.setReadTimeout(5000);
        this.restTemplate = new RestTemplate(factory);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        loadSavedWorkers();
        clusterResyncService.forceResync();
    }

    private void loadSavedWorkers() {
        var saved = statePersistenceService.loadState();
        if (saved == null) return;

        workers.clear();
        workers.putAll(saved.getWorkers());

        for (WorkerNode node : workers.values()) {
            node.setActive(true);
            node.updateHeartbeat();
            registry.restoreWorker(node.getWorkerId(), node.getUrl());
        }

        workerUrls.clear();
        workerUrls.addAll(saved.getWorkerUrls());

        for (WorkerNode node : workers.values()) {
            node.setActive(true);
            node.updateHeartbeat();
            registry.restoreWorker(node.getWorkerId(), node.getUrl());
        }
    }

    private String randomId() {
        return "worker-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public synchronized String registerNewWorker(String url) {
        WorkerNode existing = workers.get(url);

        if (existing != null) {
            existing.updateHeartbeat();
            existing.setActive(true);
            registry.updateHeartbeat(existing.getWorkerId());
            saveStateAsync();
            clusterResyncService.resyncCluster();
            return existing.getWorkerId();
        }

        String id = randomId();
        WorkerNode node = new WorkerNode(url, id);
        node.setActive(true);
        node.updateHeartbeat();

        workers.put(url, node);
        workerUrls.add(url);
        registry.register(id, url);

        saveStateAsync();
        clusterResyncService.resyncCluster();
        return id;
    }

    public void updateHeartbeat(String url) {
        WorkerNode node = workers.get(url);
        if (node == null) return;

        boolean wasInactive = !node.isActive();

        node.updateHeartbeat();
        node.setActive(true);
        registry.updateHeartbeat(node.getWorkerId());

        if (wasInactive)
            clusterResyncService.resyncCluster();

        saveStateAsync();
    }

    public boolean isWorkerHealthy(String url) {
        WorkerNode node = workers.get(url);
        if (node == null) return false;

        if (node.isHeartbeatStale(heartbeatTimeoutSeconds)) {
            node.setActive(false);
            return false;
        }
        return node.isActive();
    }

    public void markWorkerAsFailed(String url) {
        WorkerNode node = workers.get(url);
        if (node == null || !node.isActive()) return;

        node.setActive(false);
        registry.markDead(node.getWorkerId());
        clusterResyncService.resyncCluster();
        saveStateAsync();
    }

    public List<String> getActiveWorkers() {
        return workerUrls.stream()
                .filter(this::isWorkerHealthy)
                .collect(Collectors.toList());
    }

    public List<String> getAllWorkers() {
        return new ArrayList<>(workerUrls);
    }

    public Collection<WorkerNode> getAllWorkerNodes() {
        return workers.values();
    }

    @Scheduled(fixedDelayString = "${controller.state.save.interval:30000}")
    public void saveStatePeriodically() {
        saveState();
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

    public String getWorkerForKey(String key) {
        var alive = getActiveWorkers();
        return alive.isEmpty() ? null : partitioningService.getWorkerForKey(key, alive);
    }
}
