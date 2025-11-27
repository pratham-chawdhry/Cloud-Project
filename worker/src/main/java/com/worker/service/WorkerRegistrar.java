package com.worker.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Service
public class WorkerRegistrar {

    @Value("${controller.url:http://localhost:8080}")
    private String controllerUrl;

    @Value("${server.port:8081}")
    private int port;

    private final RestTemplate rest = new RestTemplate();

    private volatile String workerId;

    public String getWorkerId() {
        return workerId;
    }

    public void registerAfterReady() {
        String workerUrl = "http://localhost:" + port;

        Map<String, String> body = new HashMap<>();
        body.put("url", workerUrl);

        try {
            // Expect controller API to return ApiResponse wrapper: {status:"success", payload:{workerId: "..."}}
            @SuppressWarnings("unchecked")
            Map<String, Object> response = rest.postForObject(controllerUrl + "/worker/register", body, Map.class);

            if (response == null || !"success".equals(response.get("status"))) {
                throw new RuntimeException("Invalid response from controller: " + response);
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) response.get("payload");
            this.workerId = (String) payload.get("workerId");

            if (this.workerId == null) {
                throw new RuntimeException("Missing workerId in controller response");
            }

            System.out.println("[WORKER] Registered with controller. workerId=" + workerId + " url=" + workerUrl);

        } catch (Exception e) {
            throw new RuntimeException("[WORKER] Failed to register with controller: " + e.getMessage(), e);
        }
    }
}
