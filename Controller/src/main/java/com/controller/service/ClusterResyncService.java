package com.controller.service;

import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class ClusterResyncService {

    private final WorkerRegistry registry;
    private final ReplicaAssignmentService assignmentService;
    private final ReplicaBroadcastService broadcastService;

    private Set<String> lastAlive = new HashSet<>();

    public ClusterResyncService(WorkerRegistry registry,
                                ReplicaAssignmentService assignmentService,
                                ReplicaBroadcastService broadcastService) {
        this.registry = registry;
        this.assignmentService = assignmentService;
        this.broadcastService = broadcastService;
    }

    public synchronized void resyncCluster() {
        List<String> alive = registry.getAliveWorkers();
        Set<String> aliveNow = new HashSet<>(alive);

        if (aliveNow.equals(lastAlive)) return;

        var assignments = assignmentService.recomputeReplicas();
        broadcastService.broadcastAssignments(assignments);

        lastAlive = aliveNow;
        System.out.println("Cluster resync: alive=" + aliveNow.size());
    }

    public synchronized void forceResync() {
        var assignments = assignmentService.recomputeReplicas();
        broadcastService.broadcastAssignments(assignments);
        lastAlive = new HashSet<>(registry.getAliveWorkers());
    }
}
