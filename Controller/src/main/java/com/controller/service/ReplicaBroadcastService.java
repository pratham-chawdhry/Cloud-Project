package com.controller.service;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

@Service
public class ReplicaBroadcastService {

    private final RestTemplate rest = new RestTemplate();

    public void broadcastAssignments(Map<String, List<String>> assignments) {
        assignments.forEach((workerUrl, replicas) -> {
            try {
                String endpoint = workerUrl + "/replicas/update";
                rest.postForEntity(endpoint, replicas, String.class);
                System.out.println("Updated replicas → " + workerUrl);
            } catch (Exception e) {
                System.err.println("Failed replica update: " + workerUrl + " → " + e.getMessage());
            }
        });
    }
}
