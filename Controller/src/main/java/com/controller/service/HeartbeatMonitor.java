package com.controller.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
public class HeartbeatMonitor {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatMonitor.class);

    // RestTemplate with sensible timeouts so the scheduler doesn't hang
    private final RestTemplate restTemplate;

    private final WorkerManager workerManager;

    /**
     * Interval controlling the scheduled method. Default 10000 ms (10 seconds).
     * The @Scheduled annotation below reads this same property by key.
     */
    @Value("${controller.heartbeat.interval:10000}")
    private long heartbeatIntervalMs;

    /**
     * Timeout (seconds) before a worker is considered expired (not used here for marking failed,
     * but typically you would use it in a more advanced implementation).
     */
    @Value("${controller.heartbeat.timeout:10}")
    private long heartbeatTimeoutSeconds;

    public HeartbeatMonitor(WorkerManager workerManager) {
        this.workerManager = workerManager;

        // configure RestTemplate with timeouts (connect and read) in ms
        int connectTimeoutMs = 2000;
        int readTimeoutMs = 3000;

        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeoutMs);
        factory.setReadTimeout(readTimeoutMs);

        this.restTemplate = new RestTemplate(factory);
    }

    @PostConstruct
    public void init() {
        log.info("HeartbeatMonitor initialized. Poll interval = {} ms, timeout = {} s",
                heartbeatIntervalMs, heartbeatTimeoutSeconds);
    }

    /**
     * Runs periodically to poll each worker's /health endpoint.
     * Uses fixedDelayString so the scheduling interval is read from properties if provided.
     */
    @Scheduled(fixedDelayString = "${controller.heartbeat.interval:10000}")
    public void checkWorkerHeartbeats() {
        List<String> allWorkers = workerManager.getAllWorkers();
        if (allWorkers == null || allWorkers.isEmpty()) {
            log.debug("No workers registered to poll.");
            return;
        }

        for (String workerUrl : allWorkers) {
            if (workerUrl == null || workerUrl.trim().isEmpty()) {
                continue;
            }

            String healthUrl = normalizeHealthUrl(workerUrl);
            try {
                // Expecting the worker to return a JSON structure like { "status": "alive", "timestamp": "..." }
                // We fetch it as a Map so we can print the fields easily.
                @SuppressWarnings("unchecked")
                Map<String, Object> resp = restTemplate.getForObject(healthUrl, Map.class);

                // print health info for this worker
                if (resp != null) {
                    Object status = resp.get("status");
                    Object timestamp = resp.get("timestamp");
                    log.info("Worker [{}] HEALTHY - status: {}, timestamp: {}", workerUrl, status, timestamp);
                } else {
                    log.info("Worker [{}] HEALTHY - response body was empty", workerUrl);
                }

                // update heartbeat in manager (so other parts of system know it's alive)
                try {
                    workerManager.updateHeartbeat(workerUrl);
                } catch (Exception e) {
                    log.warn("Failed to update heartbeat for {}: {}", workerUrl, e.getMessage());
                }

            } catch (RestClientException e) {
                // didn't get a successful response from worker
                log.warn("Worker [{}] UNHEALTHY (failed to call {}): {}", workerUrl, healthUrl, e.getMessage());
            } catch (Exception e) {
                log.error("Unexpected error while checking worker [{}]: {}", workerUrl, e.getMessage(), e);
            }
        }
    }

    private String normalizeHealthUrl(String workerUrl) {
        String trimmed = workerUrl.trim();
        // remove trailing slash if present
        if (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        // append /health
        return trimmed + "/health";
    }
}
