package com.worker.service;

import org.springframework.stereotype.Component;

@Component
public class KafkaHeartbeatGate {

    private volatile boolean enabled = false;

    public void enableKafkaHeartbeats() {
        enabled = true;
        System.out.println("[WORKER] Kafka heartbeats enabled");
    }

    public boolean isEnabled() {
        return enabled;
    }
}
