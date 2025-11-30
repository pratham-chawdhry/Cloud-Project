package com.controller.service;

import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class ClusterResyncService {

    private final WorkerRegistry registry;
    private final ReplicaAssignmentService assignmentService;
    private final ReplicationManager replication;

    private Set<String> lastAlive = new HashSet<>();

    public ClusterResyncService(WorkerRegistry registry,
                                ReplicaAssignmentService assignmentService,
                                ReplicationManager replication) {
        this.registry = registry;
        this.assignmentService = assignmentService;
        this.replication = replication;
    }

    public synchronized void resyncCluster() {
        List<String> alive = registry.getAliveWorkerUrls();
        Set<String> aliveSet = new HashSet<>(alive);

        // detect worker recovery (new alive worker that wasn't there before)
        for (String id : alive) {
            if (!lastAlive.contains(id)) {
                String url = registry.getUrl(id);
                if (url != null) {
                    System.out.println("Detected worker recovery: " + url);
                    replication.recoverWorker(url);
                }
            }
        }

        if (!aliveSet.equals(lastAlive)) {
            var assignments = assignmentService.recomputeReplicas();
            broadcast(assignments);
        }

        lastAlive = aliveSet;
    }

    public synchronized void forceResync() {
        var assignments = assignmentService.recomputeReplicas();
        broadcast(assignments);

        // full replication after controller restarts
        replication.recoverAll();

        lastAlive = new HashSet<>(registry.getAliveWorkerUrls());
    }

    private void broadcast(Map<String, List<String>> assignments) {
        assignments.forEach((workerUrl, replicas) -> {
            try {
                new org.springframework.web.client.RestTemplate()
                        .postForEntity(workerUrl + "/replicas/update", replicas, Void.class);
            } catch (Exception e) {
                System.err.println("Failed to broadcast replicas to " + workerUrl + ": " + e.getMessage());
            }
        });
    }
}
