package com.controller.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import jakarta.annotation.PostConstruct;
import java.util.List;

@Service
public class HeartbeatMonitor {

    private final RestTemplate restTemplate = new RestTemplate();

    @Autowired
    private WorkerManager workerManager;

    @Value("${controller.heartbeat.interval:5000}")
    private long heartbeatIntervalMs;

    @Value("${controller.heartbeat.timeout:10}")
    private long heartbeatTimeoutSeconds;

    /**
     * Periodically checks worker heartbeats and marks failed workers.
     */
    @Scheduled(fixedDelayString = "${controller.heartbeat.interval:5000}")
    public void checkWorkerHeartbeats() {
        List<String> allWorkers = workerManager.getAllWorkers();
        
        for (String workerUrl : allWorkers) {
            try {
                // Check if worker responds to health endpoint
                restTemplate.getForEntity(workerUrl + "/health", Object.class);
                // Worker is healthy, update heartbeat
                workerManager.updateHeartbeat(workerUrl);
            } catch (RestClientException e) {
                // Worker is not responding
                // Check if worker was previously healthy
                if (workerManager.isWorkerHealthy(workerUrl)) {
                    // Worker was healthy but didn't respond, mark as failed
                    System.out.println("Worker " + workerUrl + " failed to respond to heartbeat");
                    workerManager.markWorkerAsFailed(workerUrl);
                }
            } catch (Exception e) {
                System.err.println("Error checking heartbeat for " + workerUrl + ": " + e.getMessage());
            }
        }
    }
}
