package com.controller.controller;

import com.controller.model.ApiResponse;
import com.controller.model.KeyValue;
import com.controller.service.WorkerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.*;

    @RestController
    @RequestMapping("/")
    public class KeyValueController {

        private static final Logger logger = LoggerFactory.getLogger(KeyValueController.class);

        private final RestTemplate restTemplate = new RestTemplate();

        @Autowired
        private WorkerManager workerManager;

        /**
         * PUT /put
         * Accepts either:
         *  - {"key":"k","value":"v"}
         *  - {"k":"v"}   (single arbitrary entry)
         */
        @PutMapping("/put")
        public ResponseEntity<ApiResponse<String>> put(@RequestBody Map<String, String> body) {
            try {
                // extract key/value from body
                Optional<Map.Entry<String, String>> entryOpt = extractSingleEntry(body);
                if (entryOpt.isEmpty()) {
                    return ResponseEntity.badRequest()
                            .body(ApiResponse.fail(400, "Key and value are required"));
                }

                Map.Entry<String, String> entry = entryOpt.get();
                String key = entry.getKey();
                String value = entry.getValue();

                if (isBlank(key) || isBlank(value)) {
                    return ResponseEntity.badRequest()
                            .body(ApiResponse.fail(400, "Key and value are required"));
                }

                String primaryWorker = workerManager.getWorkerForKey(key);
                if (primaryWorker == null) {
                    logger.warn("No primary worker available for key={}", key);
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                            .body(ApiResponse.fail(503, "No active workers available"));
                }

                // build request payload for worker
                Map<String, String> request = new HashMap<>();
                request.put("key", key);
                request.put("value", value);

                try {
                    logger.debug("Attempting PUT to primary worker {} for key={}", primaryWorker, key);
                    ResponseEntity<Map> response = restTemplate.postForEntity(primaryWorker + "/put", request, Map.class);
                    
                    if (response != null && response.getStatusCode().is2xxSuccessful()) {
                        logger.info("Stored key={} on worker={}", key, primaryWorker);
                        return ResponseEntity.ok(ApiResponse.success(200, "Stored key=" + key + " on worker=" + primaryWorker));
                    } else {
                        logger.warn("Primary worker {} returned non-2xx for key={}: status={}", primaryWorker, key,
                                response != null ? response.getStatusCode() : "null-response");
                        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                                .body(ApiResponse.fail(503, "Primary worker failed to store key"));
                    }
                } catch (RestClientException re) {
                    logger.warn("PUT to primary worker {} failed for key={}: {}", primaryWorker, key, re.getMessage());
                    // If primary is down, we should probably trigger a health check or let the background monitor handle it.
                    // For now, fail the request.
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                            .body(ApiResponse.fail(503, "Failed to connect to primary worker: " + re.getMessage()));
                }
            } catch (Exception e) {
                logger.error("Unexpected error in put: {}", e.getMessage(), e);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(ApiResponse.fail(500, "Internal server error: " + e.getMessage()));
            }
        }

        /**
         * POST /get
         * Accepts either:
         *  - {"key":"k"}
         *  - {"k":"<ignored-value>"}  (single-entry map; key is the map key)
         */
        @PostMapping("/get")
        public ResponseEntity<ApiResponse<KeyValue>> get(@RequestBody Map<String, String> body) {
            try {
                Optional<String> keyOpt = extractSingleKey(body);
                if (keyOpt.isEmpty() || isBlank(keyOpt.get())) {
                    return ResponseEntity.badRequest()
                            .body(ApiResponse.fail(400, "Key is required"));
                }

                String key = keyOpt.get();

                String primaryWorker = workerManager.getWorkerForKey(key);
                List<String> activeWorkers = workerManager.getActiveWorkers();
                if ((activeWorkers == null || activeWorkers.isEmpty()) && primaryWorker == null) {
                    logger.warn("No active workers available for get key={}", key);
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                            .body(ApiResponse.fail(503, "No active workers available"));
                }

                // Create ordered attempt list: primary first (if not null), then others
                LinkedHashSet<String> attemptOrder = new LinkedHashSet<>();
                if (primaryWorker != null) attemptOrder.add(primaryWorker);
                if (activeWorkers != null) {
                    for (String w : activeWorkers) attemptOrder.add(w);
                }

                Map<String, String> request = new HashMap<>();
                request.put("key", key);

                for (String worker : attemptOrder) {
                    if (isBlank(worker)) continue;
                    try {
                        logger.debug("Attempting GET from worker {} for key={}", worker, key);
                        ResponseEntity<Map> response = restTemplate.postForEntity(worker + "/get", request, Map.class);
                        if (response != null && response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                            Map<String, Object> responseBody = response.getBody();
                            Object statusObj = responseBody.get("status");
                            String status = statusObj != null ? statusObj.toString() : null;

                            if ("success".equalsIgnoreCase(status) && responseBody.containsKey("payload")) {
                                Object payloadObj = responseBody.get("payload");
                                if (payloadObj instanceof Map) {
                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> payloadMap = (Map<String, Object>) payloadObj;
                                    Object valObj = payloadMap.get("value");
                                    String value = valObj != null ? valObj.toString() : null;
                                    KeyValue keyValue = new KeyValue();
                                    keyValue.setKey(key);
                                    keyValue.setValue(value);
                                    logger.info("Retrieved key={} from worker={}", key, worker);
                                    return ResponseEntity.ok(ApiResponse.success(200, keyValue));
                                } else {
                                    logger.warn("Invalid payload format from worker {} for key={}", worker, key);
                                    // try next worker
                                }
                            } else {
                                // Either "status" not success or no payload; try next worker
                                logger.debug("Worker {} responded with status={} for key={}", worker, status, key);
                            }
                        } else {
                            logger.warn("Worker {} returned non-2xx or empty body for key={}", worker,
                                    response != null ? response.getStatusCode() : "null-response");
                        }
                    } catch (RestClientException re) {
                        logger.warn("GET from worker {} failed for key={}: {}", worker, key, re.getMessage());
                        // try next worker
                    }
                }

                // none returned the key
                logger.info("Key {} not found on any worker", key);
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(ApiResponse.fail(404, "Key not found on any worker"));
            } catch (Exception e) {
                logger.error("Unexpected error in get: {}", e.getMessage(), e);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(ApiResponse.fail(500, "Internal server error: " + e.getMessage()));
            }
        }

        // -------------------------
        // Helper utilities
        // -------------------------

        /**
         * Extract a single key/value pair from the request body.
         * Acceptable forms:
         *  - {"key":"k","value":"v"}  -> returns entry(k,v)
         *  - {"k":"v"}   -> returns that single entry
         *  - otherwise Optional.empty()
         */
        private Optional<Map.Entry<String, String>> extractSingleEntry(Map<String, String> body) {
            if (body == null || body.isEmpty()) {
                return Optional.empty();
            }

            // explicit fields
            if (body.containsKey("key") && body.containsKey("value")) {
                String k = body.get("key");
                String v = body.get("value");
                return Optional.of(new AbstractMap.SimpleEntry<>(k, v));
            }

            // if single arbitrary entry provided: {"189":"fd"}
            if (body.size() == 1) {
                Map.Entry<String, String> entry = body.entrySet().iterator().next();
                return Optional.of(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()));
            }

            // ambiguous/multi-entry request — we choose to reject here
            return Optional.empty();
        }

        /**
         * Extract a single key for GET.
         * Acceptable forms:
         *  - {"key":"k"} -> returns "k"
         *  - {"k":"..."} -> returns the map key "k"
         */
        private Optional<String> extractSingleKey(Map<String, String> body) {
            if (body == null || body.isEmpty()) {
                return Optional.empty();
            }

            if (body.containsKey("key")) {
                return Optional.ofNullable(body.get("key"));
            }

            if (body.size() == 1) {
                return Optional.ofNullable(body.entrySet().iterator().next().getKey());
            }

            return Optional.empty();
        }

        private boolean isBlank(String s) {
            return s == null || s.trim().isEmpty();
        }
    }
