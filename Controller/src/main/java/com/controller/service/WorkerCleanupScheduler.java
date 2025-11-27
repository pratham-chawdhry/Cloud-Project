package com.controller.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class WorkerCleanupScheduler {

    private final WorkerRegistry registry;
    private final ClusterResyncService clusterResyncService;

    @Value("${controller.heartbeat.timeout:15}")
    private long timeoutSeconds;

    public WorkerCleanupScheduler(WorkerRegistry registry,
                                  ClusterResyncService clusterResyncService) {
        this.registry = registry;
        this.clusterResyncService = clusterResyncService;
    }

    @Scheduled(fixedRate = 3000)
    public void checkCluster() {
        long now = System.currentTimeMillis();
        boolean changed = false;

        for (String worker : registry.getAllWorkers()) {
            Long last = registry.getLastHeartbeat(worker);
            if (last == null) continue;

            if (now - last > timeoutSeconds * 1000) {
                if (registry.markDead(worker)) {
                    System.out.println("Worker DEAD → " + worker);
                    changed = true;
                }
            }
        }

        if (changed)
            clusterResyncService.resyncCluster();
    }
}
