package com.worker.service;

import lombok.Getter;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class ReplicationService {

    private final RestTemplate restTemplate = new RestTemplate();
    @Getter
    private final List<String> replicas = new CopyOnWriteArrayList<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public void setReplicas(List<String> newReplicas) {
        replicas.clear();
        replicas.addAll(newReplicas);
    }

    public void replicateToReplicas(String key, String value) {
        if (replicas.isEmpty()) return;

        String primaryReplica = replicas.get(0);
        try {
            restTemplate.postForEntity(
                    primaryReplica + "/replicate",
                    Map.of("key", key, "value", value),
                    String.class
            );
            System.out.println("Synchronous replication succeeded to " + primaryReplica);
        } catch (Exception e) {
            System.err.println("Synchronous replication failed to " + primaryReplica + ": " + e.getMessage());
        }

        if (replicas.size() > 1) {
            String secondaryReplica = replicas.get(1);
            executor.submit(() -> {
                try {
                    restTemplate.postForEntity(
                            secondaryReplica + "/replicate",
                            Map.of("key", key, "value", value),
                            String.class
                    );
                    System.out.println("Asynchronous replication succeeded to " + secondaryReplica);
                } catch (Exception e) {
                    System.err.println("Asynchronous replication failed to " + secondaryReplica + ": " + e.getMessage());
                }
            });
        }
    }
}
