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

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Service
public class HeartbeatMonitor {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatMonitor.class);

    private final RestTemplate restTemplate;
    private final WorkerManager workerManager;

    @Value("${controller.heartbeat.interval:10000}")
    private long heartbeatIntervalMs;

    public HeartbeatMonitor(WorkerManager workerManager) {
        this.workerManager = workerManager;

        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(2000);
        factory.setReadTimeout(3000);

        this.restTemplate = new RestTemplate(factory);
    }

    @PostConstruct
    public void init() {
        log.info("HeartbeatMonitor initialized, poll={}ms", heartbeatIntervalMs);
    }

    @Scheduled(fixedDelayString = "${controller.heartbeat.interval:10000}")
    public void checkWorkerHeartbeats() {

        List<String> workers = workerManager.getAllWorkers();
        if (workers.isEmpty()) return;

        for (String workerUrl : workers) {

            String healthUrl = normalizeHealthUrl(workerUrl);

            boolean wasActive = workerManager.isWorkerActive(workerUrl);
            boolean nowActive = probeHealth(healthUrl);

            if (nowActive && !wasActive) {
                log.info("[UP] Worker {} at {}", workerUrl, Instant.now());
                workerManager.markWorkerAsRecovered(workerUrl);
            }
            else if (!nowActive && wasActive) {
                log.warn("[DOWN] Worker {} at {}", workerUrl, Instant.now());
                workerManager.markWorkerAsFailed(workerUrl);
            }
        }
    }

    private boolean probeHealth(String healthUrl) {
        try {
            Map<String,Object> resp = restTemplate.getForObject(healthUrl, Map.class);
            return resp != null;
        } catch (RestClientException e) {
            return false;
        }
    }

    private String normalizeHealthUrl(String workerUrl) {
        if (workerUrl.endsWith("/"))
            return workerUrl.substring(0, workerUrl.length() - 1) + "/health";
        return workerUrl + "/health";
    }
}
