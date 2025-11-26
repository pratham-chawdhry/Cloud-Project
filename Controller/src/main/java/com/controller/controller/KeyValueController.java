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

    /* ============================================================
       PUT — STORE KEY VALUE
    ============================================================= */
    @PutMapping("/put")
    public ResponseEntity<ApiResponse<String>> put(@RequestBody Map<String, String> body) {
        try {
            Optional<Map.Entry<String, String>> entryOpt = extractSingleEntry(body);
            if (entryOpt.isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "Key and value are required"));
            }

            String key = entryOpt.get().getKey();
            String value = entryOpt.get().getValue();

            if (isBlank(key) || isBlank(value)) {
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "Key and value are required"));
            }

            // find primary worker
            String primary = workerManager.getWorkerForKey(key);
            if (primary == null || !workerManager.isWorkerActive(primary)) {
                logger.warn("Primary worker unavailable for PUT key={}", key);
                return ResponseEntity.status(503)
                        .body(ApiResponse.fail(503, "No active workers available"));
            }

            Map<String, String> req = Map.of("key", key, "value", value);

            // send to ONLY primary worker — workers themselves replicate
            try {
                restTemplate.postForEntity(primary + "/put", req, Map.class);
                logger.info("PUT key={} stored on primary {}", key, primary);
                return ResponseEntity.ok(ApiResponse.success(200, "Stored key=" + key));
            } catch (RestClientException e) {
                logger.error("PUT failed on primary {} for key={}: {}", primary, key, e.getMessage());
                return ResponseEntity.status(503)
                        .body(ApiResponse.fail(503, "Primary worker failed"));
            }

        } catch (Exception e) {
            logger.error("Internal error in PUT: {}", e.getMessage());
            return ResponseEntity.status(500)
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }


    /* ============================================================
       GET — RETRIEVE KEY VALUE
    ============================================================= */
    @PostMapping("/get")
    public ResponseEntity<ApiResponse<KeyValue>> get(@RequestBody Map<String, String> body) {
        try {
            Optional<String> keyOpt = extractSingleKey(body);
            if (keyOpt.isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "Key is required"));
            }

            String key = keyOpt.get();

            String primary = workerManager.getWorkerForKey(key);
            List<String> active = workerManager.getActiveWorkers();

            if (primary == null && (active == null || active.isEmpty())) {
                return ResponseEntity.status(503)
                        .body(ApiResponse.fail(503, "No active workers available"));
            }

            LinkedHashSet<String> attemptWorkers = new LinkedHashSet<>();
            if (primary != null) attemptWorkers.add(primary);
            if (active != null) attemptWorkers.addAll(active);

            Map<String, String> req = Map.of("key", key);

            for (String worker : attemptWorkers) {
                if (!workerManager.isWorkerActive(worker)) continue;

                try {
                    ResponseEntity<Map> response =
                            restTemplate.postForEntity(worker + "/get", req, Map.class);

                    if (response.getStatusCode().is2xxSuccessful()
                            && response.getBody() != null
                            && "success".equalsIgnoreCase("" + response.getBody().get("status"))) {

                        Map bodyMap = response.getBody();
                        Map<String, Object> payload = (Map<String, Object>) bodyMap.get("payload");

                        String value = payload.get("value").toString();

                        KeyValue kv = new KeyValue(key, value);
                        logger.info("GET key={} served by worker={}", key, worker);

                        return ResponseEntity.ok(ApiResponse.success(200, kv));
                    }

                } catch (RestClientException e) {
                    logger.warn("GET failed on worker {}: {}", worker, e.getMessage());
                }
            }

            logger.info("GET key={} not found on any worker", key);
            return ResponseEntity.status(404)
                    .body(ApiResponse.fail(404, "Key not found"));

        } catch (Exception e) {
            logger.error("Internal error in GET: {}", e.getMessage());
            return ResponseEntity.status(500)
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }


    /* ============================================================
       HELPER UTILS
    ============================================================= */

    private Optional<Map.Entry<String, String>> extractSingleEntry(Map<String, String> body) {
        if (body == null || body.isEmpty()) return Optional.empty();

        if (body.containsKey("key") && body.containsKey("value")) {
            return Optional.of(Map.entry(body.get("key"), body.get("value")));
        }

        if (body.size() == 1) {
            Map.Entry<String, String> e = body.entrySet().iterator().next();
            return Optional.of(e);
        }

        return Optional.empty();
    }

    private Optional<String> extractSingleKey(Map<String, String> body) {
        if (body == null || body.isEmpty()) return Optional.empty();

        if (body.containsKey("key")) return Optional.ofNullable(body.get("key"));

        if (body.size() == 1) {
            return Optional.of(body.keySet().iterator().next());
        }

        return Optional.empty();
    }

    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
