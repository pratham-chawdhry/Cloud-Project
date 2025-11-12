package com.controller.controller;

import com.controller.model.ApiResponse;
import com.controller.model.KeyValue;
import com.controller.service.WorkerManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/")
public class KeyValueController {

    private final RestTemplate restTemplate = new RestTemplate();

    @Autowired
    private WorkerManager workerManager;

    @PutMapping("/put")
    public ResponseEntity<ApiResponse<String>> put(@RequestBody Map<String, String> body) {
        try {
            String key = body.get("key");
            String value = body.get("value");
            
            if (key == null || value == null) {
                return ResponseEntity.badRequest()
                    .body(ApiResponse.fail(400, "Key and value are required"));
            }

            // Get the primary worker for this key
            String primaryWorker = workerManager.getWorkerForKey(key);
            
            if (primaryWorker == null) {
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(ApiResponse.fail(503, "No active workers available"));
            }

            // Send put request to primary worker
            Map<String, String> request = new HashMap<>();
            request.put("key", key);
            request.put("value", value);

            try {
                ResponseEntity<Map> response = restTemplate.postForEntity(
                    primaryWorker + "/put",
                    request,
                    Map.class
                );

                if (response.getStatusCode().is2xxSuccessful()) {
                    // Put operation succeeded on primary worker
                    // Replication is handled by the worker itself
                    return ResponseEntity.ok(ApiResponse.success(200, 
                        "Stored key=" + key + " on worker=" + primaryWorker));
                } else {
                    return ResponseEntity.status(response.getStatusCode())
                        .body(ApiResponse.fail(response.getStatusCode().value(), 
                            "Failed to store key on worker"));
                }
            } catch (RestClientException e) {
                // If primary worker fails, try to get another worker
                List<String> activeWorkers = workerManager.getActiveWorkers();
                if (activeWorkers.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(ApiResponse.fail(503, "No active workers available"));
                }
                
                // Try the next worker in the list
                String nextWorker = workerManager.getWorkerForKey(key);
                try {
                    ResponseEntity<Map> response = restTemplate.postForEntity(
                        nextWorker + "/put",
                        request,
                        Map.class
                    );
                    return ResponseEntity.ok(ApiResponse.success(200, 
                        "Stored key=" + key + " on worker=" + nextWorker));
                } catch (RestClientException ex) {
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(ApiResponse.fail(503, "Failed to store key: " + ex.getMessage()));
                }
            }
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    @PostMapping("/get")
    public ResponseEntity<ApiResponse<KeyValue>> get(@RequestBody Map<String, String> body) {
        try {
            String key = body.get("key");
            
            if (key == null) {
                return ResponseEntity.badRequest()
                    .body(ApiResponse.fail(400, "Key is required"));
            }

            // Get the primary worker for this key
            String primaryWorker = workerManager.getWorkerForKey(key);
            
            if (primaryWorker == null) {
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(ApiResponse.fail(503, "No active workers available"));
            }

            // Send get request to primary worker
            Map<String, String> request = new HashMap<>();
            request.put("key", key);

            try {
                ResponseEntity<Map> response = restTemplate.postForEntity(
                    primaryWorker + "/get",
                    request,
                    Map.class
                );

                if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                    Map<String, Object> responseBody = response.getBody();
                    String status = (String) responseBody.get("status");
                    if ("success".equals(status) && responseBody.containsKey("payload")) {
                        // Extract key-value from payload
                        Object payloadObj = responseBody.get("payload");
                        if (payloadObj instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> payloadMap = (Map<String, Object>) payloadObj;
                            String value = payloadMap.get("value") != null ? payloadMap.get("value").toString() : null;
                            KeyValue keyValue = new KeyValue();
                            keyValue.setKey(key);
                            keyValue.setValue(value);
                            return ResponseEntity.ok(ApiResponse.success(200, keyValue));
                        } else {
                            return ResponseEntity.status(404)
                                .body(ApiResponse.fail(404, "Invalid response format from worker"));
                        }
                    } else {
                        String errorMsg = (String) responseBody.get("errorMessage");
                        return ResponseEntity.status(404)
                            .body(ApiResponse.fail(404, errorMsg != null ? errorMsg : "Key not found"));
                    }
                } else {
                    return ResponseEntity.status(response.getStatusCode())
                        .body(ApiResponse.fail(response.getStatusCode().value(), 
                            "Failed to retrieve key from worker"));
                }
            } catch (RestClientException e) {
                // If primary worker fails, try to get from replica workers
                List<String> activeWorkers = workerManager.getActiveWorkers();
                for (String worker : activeWorkers) {
                    if (worker.equals(primaryWorker)) {
                        continue; // Skip the primary worker we already tried
                    }
                    try {
                        ResponseEntity<Map> response = restTemplate.postForEntity(
                            worker + "/get",
                            request,
                            Map.class
                        );
                        
                        if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                            Map<String, Object> responseBody = response.getBody();
                            String status = (String) responseBody.get("status");
                            if ("success".equals(status) && responseBody.containsKey("payload")) {
                                Object payloadObj = responseBody.get("payload");
                                if (payloadObj instanceof Map) {
                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> payloadMap = (Map<String, Object>) payloadObj;
                                    String value = payloadMap.get("value") != null ? payloadMap.get("value").toString() : null;
                                    KeyValue keyValue = new KeyValue();
                                    keyValue.setKey(key);
                                    keyValue.setValue(value);
                                    return ResponseEntity.ok(ApiResponse.success(200, keyValue));
                                }
                                // If payload is not a Map, continue to next worker
                            }
                        }
                    } catch (RestClientException ex) {
                        // Continue to next worker
                        continue;
                    }
                }
                
                return ResponseEntity.status(404)
                    .body(ApiResponse.fail(404, "Key not found on any worker"));
            }
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.fail(500, e.getMessage()));
        }
    }
}
