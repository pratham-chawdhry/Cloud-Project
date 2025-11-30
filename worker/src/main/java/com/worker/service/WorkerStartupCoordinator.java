package com.worker.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class WorkerStartupCoordinator {

    @Autowired
    private WorkerRegistrar registrar;

    @Autowired
    private KafkaHeartbeatGate kafkaHeartbeatGate;

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        System.out.println("[WORKER] ApplicationReadyEvent received: worker is fully started");

        // 1) Register with controller (must succeed or throw)
        registrar.registerAfterReady();

        // 3) Enable Kafka heartbeats now worker is registered
        kafkaHeartbeatGate.enableKafkaHeartbeats();

        System.out.println("[WORKER] Startup sequence complete");
    }
}
