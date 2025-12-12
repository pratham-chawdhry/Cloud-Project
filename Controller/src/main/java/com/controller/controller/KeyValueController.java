package com.controller.controller;

import com.controller.model.ApiResponse;
import com.controller.model.KeyMetadata;
import com.controller.model.KeyValue;
import com.controller.service.MetadataStore;
import com.controller.service.WorkerManager;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private MetadataStore metadataStore;

    @PutMapping("/put")
    public ResponseEntity<ApiResponse<String>> put(@RequestBody Map<String, String> body) {
        try {
            Optional<Map.Entry<String, String>> entryOpt = extractSingleEntry(body);
            if (entryOpt.isEmpty())
                return fail(400, "Key and value are required");

            var entry = entryOpt.get();
            String key = entry.getKey();
            String value = entry.getValue();

            if (isBlank(key) || isBlank(value))
                return fail(400, "Key and value are required");

            // Determine primary for key
            KeyMetadata meta = metadataStore.get(key);
            String primaryWorker;

            if (meta == null) {
                primaryWorker = workerManager.getWorkerForKey(key);
                if (primaryWorker == null)
                    return fail(503, "No active workers available");
            } else {
                primaryWorker = meta.getPrimaryReplica();
            }

            Map<String, String> request = Map.of("key", key, "value", value);

            ResponseEntity<Map> response =
                    restTemplate.postForEntity(primaryWorker + "/put", request, Map.class);

            if (response == null || !response.getStatusCode().is2xxSuccessful())
                return fail(503, "Primary worker failed to store key");

            Map<String, Object> resBody = response.getBody();
            if (resBody == null) return fail(503, "Invalid worker response");

            Map<String, Object> payload = (Map<String, Object>) resBody.get("payload");
            if (payload == null)
                return fail(503, "Worker sent empty payload");

            // Worker sends sync replica
            String syncReplica = (String) payload.get("syncReplica");

            KeyMetadata newMeta = new KeyMetadata(primaryWorker, syncReplica, null);
            metadataStore.update(key, newMeta);

            return ok("Stored key=" + key + " on primary=" + primaryWorker + ", sync=" + syncReplica);

        } catch (RestClientException re) {
            return fail(503, "Failed to connect to primary worker: " + re.getMessage());
        } catch (Exception e) {
            return fail(500, "Internal server error: " + e.getMessage());
        }
    }

    @PostMapping("/get")
    public ResponseEntity<ApiResponse<Map<String, Object>>> get(@RequestBody Map<String, String> body) {
        try {
            Optional<String> keyOpt = extractSingleKey(body);
            if (keyOpt.isEmpty() || isBlank(keyOpt.get()))
                return ResponseEntity.status(400)
                        .body(ApiResponse.fail(400, "Key is required"));

            String key = keyOpt.get();

            KeyMetadata meta = metadataStore.get(key);
            if (meta == null || meta.getPrimaryReplica() == null)
                return ResponseEntity.status(404)
                        .body(ApiResponse.fail(404, "No metadata available for key=" + key));

            String primaryUrl = meta.getPrimaryReplica();

            Map<String, String> req = Map.of("key", key);

            // FETCH FULL DATA FROM PRIMARY
            ResponseEntity<Map> response =
                    restTemplate.postForEntity(primaryUrl + "/get", req, Map.class);

            if (response == null || !response.getStatusCode().is2xxSuccessful())
                return ResponseEntity.status(503)
                        .body(ApiResponse.fail(503, "Primary replica unreachable"));

            Map<String, Object> bodyMap = response.getBody();
            if (bodyMap == null || !bodyMap.containsKey("payload"))
                return ResponseEntity.status(503)
                        .body(ApiResponse.fail(503, "Invalid response from primary"));

            // The payload received from the primary worker
            Map<String, Object> fullData = (Map<String, Object>) bodyMap.get("payload");

            // Return EXACT FULL DATA coming from primary
            return ResponseEntity.ok(ApiResponse.success(200, fullData));

        } catch (Exception e) {
            return ResponseEntity.status(500)
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }


    @PostMapping("/notify/async")
    public ResponseEntity<ApiResponse<String>> notifyAsync(
            @RequestBody Map<String, String> body,
            HttpServletRequest request) {

        try {
            String key = body.get("key");
            if (key == null || key.isBlank())
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "Key required"));

            String asyncWorkerUrl = body.get("worker");
            if (asyncWorkerUrl == null || asyncWorkerUrl.isBlank())
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "Worker URL missing"));

            KeyMetadata meta = metadataStore.get(key);
            if (meta == null)
                return ResponseEntity.status(404)
                        .body(ApiResponse.fail(404, "No metadata for key=" + key));

            meta.setAsyncReplica(asyncWorkerUrl);
            metadataStore.update(key, meta);

            return ResponseEntity.ok(ApiResponse.success(
                    200, "Async replica recorded key=" + key + " worker=" + asyncWorkerUrl));

        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    @PostMapping("/notify/primary")
    public ResponseEntity<ApiResponse<String>> notifyPrimary(@RequestBody Map<String, String> body) {

        try {
            String key = body.get("key");
            String newPrimary = body.get("worker");

            if (key == null || key.isBlank())
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "Key required"));

            if (newPrimary == null || newPrimary.isBlank())
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "Worker URL missing"));

            KeyMetadata meta = metadataStore.get(key);

            if (meta == null)
                meta = new KeyMetadata(newPrimary, null, null);
            else
                meta.setPrimaryReplica(newPrimary);

            metadataStore.update(key, meta);

            return ResponseEntity.ok(
                    ApiResponse.success(
                            200,
                            "Primary updated key=" + key + " newPrimary=" + newPrimary
                    )
            );

        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    private Optional<Map.Entry<String, String>> extractSingleEntry(Map<String, String> body) {
        if (body == null || body.isEmpty()) return Optional.empty();
        if (body.containsKey("key") && body.containsKey("value"))
            return Optional.of(Map.entry(body.get("key"), body.get("value")));
        if (body.size() == 1) {
            var e = body.entrySet().iterator().next();
            return Optional.of(Map.entry(e.getKey(), e.getValue()));
        }
        return Optional.empty();
    }

    private Optional<String> extractSingleKey(Map<String, String> body) {
        if (body == null || body.isEmpty()) return Optional.empty();
        if (body.containsKey("key")) return Optional.of(body.get("key"));
        if (body.size() == 1) return Optional.of(body.keySet().iterator().next());
        return Optional.empty();
    }

    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private ResponseEntity<ApiResponse<String>> fail(int code, String msg) {
        return ResponseEntity.status(code).body(ApiResponse.fail(code, msg));
    }

    private ResponseEntity<ApiResponse<String>> ok(String msg) {
        return ResponseEntity.ok(ApiResponse.success(200, msg));
    }

    private ResponseEntity<ApiResponse<KeyValue>> failKV(int code, String msg) {
        return ResponseEntity.status(code).body(ApiResponse.fail(code, msg));
    }
}
