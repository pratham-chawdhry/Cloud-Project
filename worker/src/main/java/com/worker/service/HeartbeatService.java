package com.worker.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
public class HeartbeatService {

    @Value("${controller.url:http://localhost:8080}")
    private String controllerUrl;

    @Value("${server.port:8081}")
    private int port;

    private final RestTemplate rest = new RestTemplate();
    private volatile boolean running = false;

    /**
     * Starts a daemon thread that sends heartbeats every 5 seconds.
     * Safe to call multiple times; only one thread will run.
     */
    public void startHeartbeatThread() {
        if (running) return;
        running = true;

        String workerUrl = "http://localhost:" + port;

        Thread t = new Thread(() -> {
            while (running) {
                try {
                    System.out.println("[WORKER] Sending HTTP heartbeat to " + controllerUrl + "/worker/heartbeat -> " + workerUrl);
                    rest.postForEntity(controllerUrl + "/worker/heartbeat", Map.of("workerUrl", workerUrl), String.class);
                } catch (Exception e) {
                    System.err.println("[WORKER] HTTP heartbeat failed: " + e.getMessage());
                }

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        t.setName("worker-http-heartbeat-thread");
        t.setDaemon(true);
        t.start();
    }

    public void stopHeartbeatThread() {
        running = false;
    }
}
