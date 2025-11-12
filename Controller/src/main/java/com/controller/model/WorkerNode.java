package com.controller.model;

import lombok.Data;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Data
public class WorkerNode {
    private String url;
    private boolean active;
    private Instant lastHeartbeat;
    private List<String> replicaUrls;
    private int workerId;

    public WorkerNode(String url, int workerId) {
        this.url = url;
        this.workerId = workerId;
        this.active = true;
        this.lastHeartbeat = Instant.now();
        this.replicaUrls = new ArrayList<>();
    }

    public void updateHeartbeat() {
        this.lastHeartbeat = Instant.now();
        this.active = true;
    }

    public boolean isHeartbeatStale(long timeoutSeconds) {
        if (lastHeartbeat == null) return true;
        return Instant.now().getEpochSecond() - lastHeartbeat.getEpochSecond() > timeoutSeconds;
    }
}
