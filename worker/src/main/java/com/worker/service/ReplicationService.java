package com.worker.service;

import lombok.Getter;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
public class ReplicationService {

    private final RestTemplate rest = new RestTemplate();
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KeyValueStore keyValueStore;
    private final WorkerInfoProvider infoProvider;
    private final WorkerRegistrar workerRegistrar;

    @Getter private String syncReplica;
    @Getter private String asyncReplica;

    public ReplicationService(
            KafkaTemplate<String, String> kafkaTemplate,
            KeyValueStore keyValueStore,
            WorkerInfoProvider infoProvider, WorkerRegistrar workerRegistrar
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.keyValueStore = keyValueStore;
        this.infoProvider = infoProvider;
        this.workerRegistrar = workerRegistrar;
    }

    public void updateReplicaTargets(String sync, String async) {
        this.syncReplica = sync;
        this.asyncReplica = async;
    }

    public void replicateSyncOrThrow(String key, String value) throws Exception {
        if (syncReplica == null)
            throw new Exception("No synchronous replica configured");

        try {
            rest.postForEntity(
                    syncReplica + "/replicate",
                    Map.of("key", key, "value", value),
                    String.class
            );
            System.out.println("SYNC → " + syncReplica);
        } catch (Exception e) {
            throw new Exception("SYNC replication failed to " + syncReplica + ": " + e.getMessage(), e);
        }
    }

    public void replicateAsync(String key, String value) {
        if (asyncReplica == null) return;

        try {
            String payload = key + "|" + value + "|" + asyncReplica;
            kafkaTemplate.send("replication-events", asyncReplica, payload);
            System.out.println("ASYNC queued → " + asyncReplica);
        } catch (Exception e) {
            System.err.println("ASYNC enqueue failed: " + e.getMessage());
        }
    }

    @KafkaListener(
            topics = "replication-events",
            groupId = "worker-replicators-${server.port}"
    )
    public void handleAsyncReplication(String message) {
        try {
            // Format → key|value|targetUrl
            String[] parts = message.split("\\|", 3);
            if (parts.length < 3) {
                System.err.println("Invalid replication payload: " + message);
                return;
            }

            String key = parts[0];
            String value = parts[1];
            String target = parts[2];

            // Only process if message is intended for THIS worker
            String myUrl = workerRegistrar.getWorkerUrl();
            System.out.println("Async replication target: " + target + ", myUrl: " + myUrl);
            if (!myUrl.equals(target)) return;

            keyValueStore.put(key, value);
            System.out.println("ASYNC REPLICATED → (" + key + "=" + value + ")");

        } catch (Exception e) {
            System.err.println("Async replication failed: " + e.getMessage());
        }
    }
}
