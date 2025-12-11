package com.controller.service;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Service
public class ClusterResyncService {

    private final RestTemplate rest = new RestTemplate();
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
        List<String> aliveList = registry.getAliveWorkerUrls();
        Set<String> aliveSet = new HashSet<>(aliveList);

        List<String> dead = lastAlive.stream()
                .filter(id -> !aliveSet.contains(id))
                .toList();

        if (!aliveSet.equals(lastAlive)) {
            broadcast(aliveSet, dead);
        }

        lastAlive = aliveSet;
    }


    public synchronized void forceResync() {
        Set<String> aliveWorkers = new HashSet<>(registry.getAliveWorkerUrls());

        broadcast(aliveWorkers, List.of());

        replication.recoverAll();
        lastAlive = aliveWorkers;
    }

    private void broadcast(Set<String> aliveWorkers, List<String> deadWorkers) {
        aliveWorkers.forEach(workerUrl -> {
            try {
                Map<String, Object> body = new HashMap<>();
                body.put("deadWorkers", deadWorkers);
                body.put("aliveWorkers", aliveWorkers);

                rest.postForEntity(
                        workerUrl + "/replicas/update",
                        body,
                        Void.class
                );

            } catch (Exception e) {
                System.err.println("Failed to broadcast cluster state to "
                        + workerUrl + ": " + e.getMessage());
            }
        });
    }
}
