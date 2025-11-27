package com.controller.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class HeartbeatConsumer {

    private final WorkerRegistry registry;
    private final ClusterResyncService clusterResyncService;

    public HeartbeatConsumer(WorkerRegistry registry,
                             ClusterResyncService clusterResyncService) {
        this.registry = registry;
        this.clusterResyncService = clusterResyncService;
    }

    @KafkaListener(topics = "worker-heartbeats", groupId = "controller-group")
    public void consume(Map<String, Object> heartbeat) {
        String workerId = (String) heartbeat.get("workerId");
        if (workerId == null) return;

        registry.updateHeartbeat(workerId);
        clusterResyncService.resyncCluster();
    }
}
