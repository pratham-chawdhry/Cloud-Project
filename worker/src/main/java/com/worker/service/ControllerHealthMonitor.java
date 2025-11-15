package com.worker.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitors controller health and detects controller failures.
 * Workers can continue operating even when controller is down,
 * but will retry connecting when controller recovers.
 */
@Service
public class ControllerHealthMonitor {

    private final RestTemplate restTemplate = new RestTemplate();
    
    @Value("${controller.url}")
    private String controllerUrl;

    @Value("${controller.health.check.interval:5000}")
    private long healthCheckIntervalMs;

    @Value("${controller.health.timeout:10000}")
    private long controllerTimeoutMs;

    private final AtomicBoolean controllerAvailable = new AtomicBoolean(true);
    private final AtomicLong lastSuccessfulContact = new AtomicLong(System.currentTimeMillis());
    private volatile boolean controllerDown = false;

    @PostConstruct
    public void init() {
        System.out.println("ControllerHealthMonitor initialized. Controller URL: " + controllerUrl);
        // Initial health check
        checkControllerHealth();
    }

    /**
     * Periodically checks if the controller is available.
     */
    @Scheduled(fixedDelayString = "${controller.health.check.interval:5000}")
    public void checkControllerHealth() {
        try {
            // Try to reach controller health endpoint
            restTemplate.getForEntity(controllerUrl + "/worker/health", Object.class);
            
            // Controller is available
            boolean wasDown = controllerDown;
            controllerAvailable.set(true);
            controllerDown = false;
            lastSuccessfulContact.set(System.currentTimeMillis());
            
            if (wasDown) {
                System.out.println("Controller is back online! Re-establishing connection...");
                onControllerRecovery();
            }
            
        } catch (RestClientException e) {
            // Controller is not responding
            if (!controllerDown) {
                System.out.println("Controller failure detected: " + e.getMessage());
                controllerDown = true;
                controllerAvailable.set(false);
                onControllerFailure();
            }
        } catch (Exception e) {
            System.err.println("Error checking controller health: " + e.getMessage());
        }
    }

    /**
     * Checks if controller is currently available.
     */
    public boolean isControllerAvailable() {
        // Also check if we haven't heard from controller in a while
        long timeSinceLastContact = System.currentTimeMillis() - lastSuccessfulContact.get();
        if (timeSinceLastContact > controllerTimeoutMs) {
            controllerAvailable.set(false);
            if (!controllerDown) {
                System.out.println("Controller timeout: No response for " + (timeSinceLastContact / 1000) + " seconds");
                controllerDown = true;
                onControllerFailure();
            }
        }
        return controllerAvailable.get() && !controllerDown;
    }

    /**
     * Gets the time since last successful contact with controller (in milliseconds).
     */
    public long getTimeSinceLastContact() {
        return System.currentTimeMillis() - lastSuccessfulContact.get();
    }

    /**
     * Called when controller failure is detected.
     */
    private void onControllerFailure() {
        System.out.println("Controller is down. Worker will continue operating but some features may be limited.");
        System.out.println("Worker will retry connecting to controller periodically.");
    }

    /**
     * Called when controller recovers.
     */
    private void onControllerRecovery() {
        System.out.println("Controller recovered. Re-syncing worker configuration...");
        // Workers can re-register or re-sync with controller here if needed
        // For now, the controller will detect workers through its heartbeat monitor
    }

    /**
     * Attempts to send a heartbeat to controller with retry logic.
     * Returns true if successful, false otherwise.
     */
    public boolean sendHeartbeatWithRetry(String workerUrl, int maxRetries) {
        if (!isControllerAvailable() && maxRetries <= 0) {
            return false;
        }

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                java.util.Map<String, String> body = new java.util.HashMap<>();
                body.put("workerUrl", workerUrl);
                
                restTemplate.postForEntity(controllerUrl + "/worker/heartbeat", body, Object.class);
                lastSuccessfulContact.set(System.currentTimeMillis());
                controllerAvailable.set(true);
                controllerDown = false;
                return true;
                
            } catch (RestClientException e) {
                if (attempt < maxRetries - 1) {
                    try {
                        // Exponential backoff: 1s, 2s, 4s, etc.
                        long delay = (long) Math.pow(2, attempt) * 1000;
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            } catch (Exception e) {
                System.err.println("Unexpected error sending heartbeat: " + e.getMessage());
                return false;
            }
        }
        
        return false;
    }
}

