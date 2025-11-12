package com.worker.service;

import lombok.Getter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
public class ReplicationService {

    private final RestTemplate restTemplate = new RestTemplate();
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Getter
    private String syncReplica;
    @Getter
    private String asyncReplica;

    public ReplicationService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void updateReplicaTargets(String sync, String async) {
        this.syncReplica = sync;
        this.asyncReplica = async;
    }

    public void replicateSyncOrThrow(String key, String value) throws Exception {
        if (syncReplica == null)
            throw new Exception("No synchronous replica configured");
        try {
            restTemplate.postForEntity(
                    syncReplica + "/replicate",
                    Map.of("key", key, "value", value),
                    String.class
            );
            System.out.println("SYNC → " + syncReplica);
        } catch (Exception e) {
            throw new Exception("Synchronous replication failed to " + syncReplica + ": " + e.getMessage(), e);
        }
    }

    public void replicateAsync(String key, String value) {
        if (asyncReplica == null) return;

        try {
            String payload = key + "|" + value + "|" + asyncReplica;
            kafkaTemplate.send("replication-events", asyncReplica, payload);
            System.out.println("ASYNC (queued) → Kafka for " + asyncReplica);
        } catch (Exception e) {
            System.err.println("ASYNC enqueue failed: " + e.getMessage());
        }
    }
}
