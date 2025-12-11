package com.worker.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;

@Service
public class HeartbeatProducer {

    private final WorkerRegistrar registrar;
    private final KafkaTemplate<String, Object> kafka;
    private final KafkaHeartbeatGate gate;

    public HeartbeatProducer(WorkerRegistrar registrar, KafkaTemplate<String, Object> kafka, KafkaHeartbeatGate gate) {
        this.registrar = registrar;
        this.kafka = kafka;
        this.gate = gate;
    }

    @Scheduled(fixedDelayString = "${worker.heartbeat.interval}")
    public void sendHeartbeat() {

        if (!gate.isEnabled()) return; // not ready yet
        if (registrar.getWorkerId() == null) return;

        var payload = Map.of(
                "workerId", registrar.getWorkerId(),
                "timestamp", Instant.now().toString(),
                "status", "alive"
        );

        try {
            kafka.send("worker-heartbeats", registrar.getWorkerId(), payload);
            System.out.println("[WORKER] Kafka heartbeat: " + payload);
        } catch (Exception e) {
            System.err.println("[WORKER] Kafka heartbeat failed: " + e.getMessage());
        }
    }
}
