package com.worker.service;

import com.worker.model.*;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Service
public class ReplicationService {

    @Value("${worker.heartbeat.interval:5000}")
    private long heartbeatInterval;

    private final RestTemplate rest = new RestTemplate();
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KeyValueStore keyValueStore;
    private final WorkerRegistrar workerRegistrar;
    private volatile Set<String> aliveWorkers = new HashSet<>();

    public ReplicationService(
            KafkaTemplate<String, Object> kafkaTemplate,
            KeyValueStore keyValueStore,
            WorkerRegistrar workerRegistrar
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.keyValueStore = keyValueStore;
        this.workerRegistrar = workerRegistrar;
    }

    public void updateClusterState(Set<String> newAliveWorkers) {
        this.aliveWorkers = newAliveWorkers;
    }

    public Set<String> getAliveWorkers() {
        return aliveWorkers;
    }

    private Map<String, String> createSyncReplica(String key,
                                                  String value,
                                                  String targetUrl,
                                                  String primaryUrl,
                                                  String asyncUrl,
                                                  boolean isPromotion) {

        try {
            if (asyncUrl != null && asyncUrl.equals(targetUrl)) {
                asyncUrl = null;
            }

            Map<String, Object> body = new HashMap<>();
            body.put("key", key);
            body.put("value", value);
            body.put("primaryUrl", primaryUrl);
            body.put("syncUrl", targetUrl);
            body.put("asyncUrl", asyncUrl);

            rest.postForEntity(targetUrl + "/replicate", body, String.class);

            System.out.println((isPromotion ?
                    "Promoted ASYNC -> SYNC: " : "Created SYNC on: ") + targetUrl);

            Map<String, String> result = new HashMap<>();
            result.put("primaryReplica", primaryUrl);
            result.put("syncReplica", targetUrl);
            result.put("asyncReplica", asyncUrl); // may be null
            return result;

        } catch (Exception e) {
            System.err.println(
                    (isPromotion ? "Promotion" : "SYNC create") +
                            " failed on " + targetUrl + ": " + e.getMessage()
            );
            return null;
        }
    }

    public String chooseAsyncCandidate(String primary,
                                        String syncCandidate) {
        for (String cand : aliveWorkers) {
            if (cand.equals(primary)) continue;
            if (cand.equals(syncCandidate)) continue;
            return cand;
        }
        return null;
    }

    public Map<String, String> syncReplicaCreate(String key,
                                                 String value,
                                                 String oldAsync) throws Exception {

        String primaryUrl = workerRegistrar.getWorkerUrl();

        if (aliveWorkers.size() <= 1)
            return null;

        long retryDelay = heartbeatInterval;
        int maxAttempts = 4;
        long endTime = System.currentTimeMillis() + (retryDelay * maxAttempts);

        List<String> candidates = new ArrayList<>(aliveWorkers);

        while (System.currentTimeMillis() < endTime) {

            for (String candidate : candidates) {

                if (candidate.equals(primaryUrl)) continue;
                if (candidate.equals(oldAsync)) continue;
                if (!aliveWorkers.contains(candidate)) continue;

                String asyncUrl = oldAsync;

                if (asyncUrl != null && asyncUrl.equals(candidate)) {
                    asyncUrl = null;
                }

                if (asyncUrl == null && aliveWorkers.size() > 2) {
                    asyncUrl = chooseAsyncCandidate(primaryUrl, candidate);
                }

                Map<String, String> result =
                        createSyncReplica(key, value, candidate, primaryUrl, asyncUrl, false);

                if (result != null)
                    return result;
            }

            if (oldAsync != null &&
                    aliveWorkers.contains(oldAsync) &&
                    !oldAsync.equals(primaryUrl)) {

                String asyncUrl = null;

                if (aliveWorkers.size() > 2) {
                    asyncUrl = chooseAsyncCandidate(primaryUrl, oldAsync);
                }

                if (asyncUrl != null && asyncUrl.equals(oldAsync)) {
                    asyncUrl = null;
                }

                Map<String, String> result =
                        createSyncReplica(key, value, oldAsync, primaryUrl, asyncUrl, true);

                if (result != null)
                    return result;
            }

            try {
                Thread.sleep(retryDelay);
            } catch (InterruptedException ignored) {}
        }

        throw new Exception("Unable to create SYNC replica for key=" + key + " after retry window");
    }

    public boolean syncUpdate(String key,
                              String value,
                              String syncUrl,
                              String asyncTarget) {

        String primaryUrl = workerRegistrar.getWorkerUrl();

        Map<String, Object> body = new HashMap<>();
        body.put("key", key);
        body.put("value", value);
        body.put("primaryUrl", primaryUrl);
        body.put("syncUrl", syncUrl);
        body.put("asyncUrl", asyncTarget);

        long retryDelay = heartbeatInterval;
        int maxRetries = 4;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            if (!aliveWorkers.contains(syncUrl)) {
                System.err.println("SYNC update aborted — controller marked " +
                        syncUrl + " as dead");
                return false;
            }

            try {
                rest.postForEntity(syncUrl + "/replicate", body, String.class);
                return true; // Success
            }
            catch (Exception e) {
                System.err.println("SYNC update attempt " + attempt + "/" + maxRetries +
                        " failed for key=" + key + " sync=" + syncUrl +
                        ": " + e.getMessage());
            }

            try {
                Thread.sleep(retryDelay);
            } catch (InterruptedException ignored) {}
        }

        System.err.println("SYNC update failed for key=" + key +
                " after " + maxRetries + " attempts");
        return false;
    }

    public boolean replicateAsync(String key, String value, String asyncTarget, String syncUrl) {
        if (asyncTarget == null || asyncTarget.isBlank()) return false;

        try {
            String primaryUrl = workerRegistrar.getWorkerUrl();

            Map<String, Object> payload = new HashMap<>();
            payload.put("key", key);
            payload.put("value", value);
            payload.put("targetUrl", asyncTarget);
            payload.put("primaryUrl", primaryUrl);
            payload.put("syncUrl", syncUrl);

            kafkaTemplate.send("replication-events", asyncTarget, payload);

            System.out.println("ASYNC queued → " + payload);
            return true;

        } catch (Exception e) {
            System.err.println("ASYNC enqueue failed for key=" + key +
                    " target=" + asyncTarget + " : " + e.getMessage());
            return false;
        }
    }


    @KafkaListener(
            topics = "replication-events",
            groupId = "worker-replicators-${server.port}"
    )
    public void handleAsyncReplication(Map<String, Object> payload) {
        try {
            String key = (String) payload.get("key");
            String value = (String) payload.get("value");
            String targetUrl = (String) payload.get("targetUrl");
            String primaryUrl = (String) payload.get("primaryUrl");
            String syncUrl = (String) payload.get("syncUrl");

            String myUrl = workerRegistrar.getWorkerUrl();

            if (!myUrl.equals(targetUrl)) return;

            KeyValue kv = new KeyValue();
            kv.setKey(key);
            kv.setValue(value);
            kv.setReplicaType(ReplicaType.ASYNC);

            ReplicaInfo info = new ReplicaInfo();
            info.setPrimaryReplica(primaryUrl);
            info.setSyncReplica(syncUrl);
            info.setAsyncReplica(myUrl);

            kv.setReplicaInfo(info);

            keyValueStore.put(kv);

            System.out.println("ASYNC STORED -> " + key + " from " + primaryUrl);

            rest.postForEntity(
                    "http://localhost:8080/notify/async",
                    Map.of("key", key, "worker", myUrl),
                    String.class
            );

        } catch (Exception e) {
            System.err.println("Async replication failed: " + e.getMessage());
        }
    }
}
