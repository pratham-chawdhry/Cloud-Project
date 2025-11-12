package com.worker.consumer;

import com.worker.service.KeyValueStore;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AsyncReplicationConsumer {

    private final KeyValueStore keyValueStore;

    public AsyncReplicationConsumer(KeyValueStore keyValueStore) {
        this.keyValueStore = keyValueStore;
    }

    @KafkaListener(
            topics = "replication-events",
            groupId = "worker-replication-group"
    )
    public void consume(String message) {
        String[] parts = message.split("\\|");
        String key = parts[0];
        String value = parts[1];
        String target = parts[2];

        if (System.getenv("WORKER_URL").equals(target)) {
            keyValueStore.put(key, value);
            System.out.println("ASYNC APPLIED → " + key + "=" + value);
        }
    }
}
