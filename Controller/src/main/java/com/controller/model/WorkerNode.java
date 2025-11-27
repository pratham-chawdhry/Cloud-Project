package com.controller.model;

import lombok.Data;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Data
public class WorkerNode {
    private String url;
    private transient boolean active;
    private transient long lastHeartbeat;
    private List<String> replicaUrls;
    private String workerId;

    public WorkerNode(String url, String workerId) {
        this.url = url;
        this.workerId = workerId;
        this.active = true;
        this.lastHeartbeat = System.currentTimeMillis();
        this.replicaUrls = new ArrayList<>();
    }

    public void updateHeartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
        this.active = true;
    }

    public boolean isHeartbeatStale(long timeoutSeconds) {
        return System.currentTimeMillis() - lastHeartbeat > timeoutSeconds * 1000L;
    }
}
