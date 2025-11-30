package com.controller.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class WorkerCleanupScheduler {

    private final WorkerRegistry registry;
    private final ClusterResyncService resync;

    @Value("${controller.heartbeat.timeout:15}")
    private long timeout;

    public WorkerCleanupScheduler(WorkerRegistry registry, ClusterResyncService resync) {
        this.registry = registry;
        this.resync = resync;
    }

    @Scheduled(fixedRate = 3000)
    public void checkCluster() {
        long now = System.currentTimeMillis();
        boolean changed = false;

        for (String id : registry.getAllWorkerIds()) {
            Long last = registry.getLastHeartbeat(id);
            if (last == null) continue;

            if (now - last > timeout * 1000) {
                String url = registry.markDead(id);
                System.out.println("DEAD WORKER → " + id + " (" + url + ")");
                changed = true;
            }
        }

        if (changed) resync.resyncCluster();
    }
}

