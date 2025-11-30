package com.controller.service;

import com.controller.service.ClusterResyncService;
import com.controller.service.WorkerRegistry;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class HeartbeatConsumer {

    private final WorkerRegistry registry;
    private final ClusterResyncService resync;

    public HeartbeatConsumer(WorkerRegistry registry,
                             ClusterResyncService resync) {
        this.registry = registry;
        this.resync = resync;
    }

    @KafkaListener(topics = "worker-heartbeats", groupId = "controller-group")
    public void consume(Map<String, Object> heartbeat) {
        String workerId = (String) heartbeat.get("workerId");
        if (workerId == null) return;

        registry.updateHeartbeat(workerId);

        // IMPORTANT: this triggers replica reassignment
        resync.resyncCluster();
    }
}
