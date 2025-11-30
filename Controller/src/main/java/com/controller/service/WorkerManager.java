package com.controller.service;

import com.controller.model.WorkerNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class WorkerManager {

    private final Map<String, WorkerNode> workers = new ConcurrentHashMap<>();
    private final List<String> workerUrls = new ArrayList<>();

    private final WorkerRegistry registry;
    private final StatePersistenceService persistence;
    private final ClusterResyncService resync;
    private final PartitioningService partition;

    @Autowired
    public WorkerManager(WorkerRegistry registry,
                         StatePersistenceService persistence,
                         ClusterResyncService resync,
                         PartitioningService partition) {
        this.registry = registry;
        this.persistence = persistence;
        this.resync = resync;
        this.partition = partition;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        loadState();
        resync.forceResync();
    }

    private void loadState() {
        var state = persistence.loadState();
        if (state == null) return;

        workers.clear();
        workers.putAll(state.getWorkers());
        workerUrls.clear();
        workerUrls.addAll(state.getWorkerUrls());

        workers.values().forEach(n -> registry.restore(n.getWorkerId(), n.getUrl()));
    }

    private String id() {
        return "worker-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public synchronized String register(String url) {
        System.out.println("[WorkerManager] register url=" + url +
                " contains=" + workers.containsKey(url) +
                " regId=" + registry.getWorkerIdByUrl(url));

        // case: stale WorkerNode but registry lost workerId → treat as new
        if (workers.containsKey(url) && registry.getWorkerIdByUrl(url) == null) {
            System.out.println("[WorkerManager] stale entry, cleaning: " + url);
            workers.remove(url);
            workerUrls.remove(url);
        }

        // existing valid
        if (workers.containsKey(url)) {
            WorkerNode n = workers.get(url);
            registry.updateHeartbeat(n.getWorkerId());
            save();
            resync.resyncCluster();
            return n.getWorkerId();
        }

        // new worker
        String wid = id();
        WorkerNode node = new WorkerNode(url, wid);
        node.updateHeartbeat();

        workers.put(url, node);
        if (!workerUrls.contains(url)) workerUrls.add(url);
        registry.register(wid, url);

        save();
        resync.resyncCluster();

        System.out.println("[WorkerManager] NEW worker id=" + wid + " url=" + url);
        return wid;
    }

    public void heartbeat(String url) {
        WorkerNode n = workers.get(url);
        if (n == null) return;

        n.updateHeartbeat();
        registry.updateHeartbeat(n.getWorkerId());
        save();
        resync.resyncCluster();
    }

    public List<String> getActiveWorkers() {
        List<String> result = new ArrayList<>();
        for (String id : registry.getAliveWorkerIds()) {
            String url = registry.getUrl(id);
            if (url != null) result.add(url);
        }
        return result;
    }

    public String getWorkerForKey(String key) {
        List<String> active = getActiveWorkers();
        if (active.isEmpty()) return null;
        return partition.getWorkerForKey(key, active);
    }

    public List<String> getAllWorkers() {
        return new ArrayList<>(workerUrls);
    }

    public Collection<WorkerNode> getWorkerNodes() {
        return workers.values();
    }

    public synchronized void removeByUrl(String url) {
        WorkerNode removed = workers.remove(url);
        boolean removedList = workerUrls.remove(url);
        System.out.println("[WorkerManager] removeByUrl url=" + url +
                " removed=" + (removed != null) +
                " removedList=" + removedList);
    }

    private void save() {
        persistence.saveState(workers, workerUrls);
    }
}
