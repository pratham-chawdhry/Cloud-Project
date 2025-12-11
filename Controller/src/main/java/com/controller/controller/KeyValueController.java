package com.controller.controller;

import com.controller.model.ApiResponse;
import com.controller.model.KeyMetadata;
import com.controller.model.KeyValue;
import com.controller.service.MetadataStore;
import com.controller.service.WorkerManager;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.kafka.clients.Metadata;
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

            String primaryWorker = workerManager.getWorkerForKey(key);
            if (primaryWorker == null)
                return fail(503, "No active workers available");

            Map<String, String> request = Map.of("key", key, "value", value);

            try {
                System.out.println(primaryWorker);
                ResponseEntity<Map> response =
                        restTemplate.postForEntity(primaryWorker + "/put", request, Map.class);

                if (response != null && response.getStatusCode().is2xxSuccessful()){
                    Map<String, Object> resBody = response.getBody();

                    if (resBody == null) return fail(503, "Invalid worker response");

                    Map<String, Object> payload = (Map<String, Object>) resBody.get("payload");
                    if (payload == null)
                        return fail(503, "Worker sent empty payload");

                    String syncReplica = (String) payload.get("syncReplica");

                    KeyMetadata m = new KeyMetadata(primaryWorker, syncReplica, null);
                    metadataStore.update(key, m);
                    return ok("Stored key=" + key + " on " + primaryWorker + " and "  + syncReplica);
                }
                return fail(503, "Primary worker failed to store key");
            } catch (RestClientException re) {
                return fail(503, "Failed to connect to primary worker: " + re.getMessage());
            }
        } catch (Exception e) {
            return fail(500, "Internal server error: " + e.getMessage());
        }
    }

    @PostMapping("/get")
    public ResponseEntity<ApiResponse<KeyValue>> get(@RequestBody Map<String, String> body) {
        try {
            Optional<String> keyOpt = extractSingleKey(body);
            if (keyOpt.isEmpty() || isBlank(keyOpt.get()))
                return failKV(400, "Key is required");

            String key = keyOpt.get();

            String primary = workerManager.getWorkerForKey(key);
            List<String> active = workerManager.getActiveWorkers();

            if (active.isEmpty() && primary == null)
                return failKV(503, "No active workers available");

            LinkedHashSet<String> workers = new LinkedHashSet<>();
            if (primary != null) workers.add(primary);
            workers.addAll(active);

            Map<String, String> request = Map.of("key", key);

            for (String worker : workers) {
                if (isBlank(worker)) continue;
                try {
                    ResponseEntity<Map> response =
                            restTemplate.postForEntity(worker + "/get", request, Map.class);

                    if (response == null || !response.getStatusCode().is2xxSuccessful())
                        continue;

                    Map<String, Object> bodyMap = response.getBody();
                    if (bodyMap == null || !bodyMap.containsKey("payload"))
                        continue;

                    Object payloadObj = bodyMap.get("payload");
                    if (!(payloadObj instanceof Map)) continue;

                    Map<String, Object> payload = (Map<String, Object>) payloadObj;
                    Object val = payload.get("value");

                    KeyValue kv = new KeyValue();
                    kv.setKey(key);
                    kv.setValue(val != null ? val.toString() : null);

                    return okKV(kv);

                } catch (RestClientException ignored) {}
            }

            return failKV(404, "Key not found");
        } catch (Exception e) {
            return failKV(500, "Internal server error: " + e.getMessage());
        }
    }

    @PostMapping("/notify/async")
    public ResponseEntity<ApiResponse<String>> notifyAsync(
            @RequestBody Map<String, String> body,
            HttpServletRequest request) {

        try {
            // 1. Key must exist
            String key = body.get("key");
            if (key == null || key.isBlank()) {
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "Key required"));
            }

            // 2. Worker URL MUST be sent explicitly by worker
            String asyncWorkerUrl = body.get("worker");
            if (asyncWorkerUrl == null || asyncWorkerUrl.isBlank()) {
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "Worker URL missing in notify/async request"));
            }

            System.out.println("Async replication completed by worker -> " + asyncWorkerUrl);

            // 3. Fetch metadata
            KeyMetadata meta = metadataStore.get(key);
            if (meta == null) {
                return ResponseEntity.status(404)
                        .body(ApiResponse.fail(404, "No metadata exists for key=" + key));
            }

            // 4. Update ONLY asyncReplica
            meta.setAsyncReplica(asyncWorkerUrl);

            // 5. Save back to metadata.json
            metadataStore.update(key, meta);

            return ResponseEntity.ok(
                    ApiResponse.success(
                            200,
                            "Async replica recorded: key=" + key + " worker=" + asyncWorkerUrl
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
        if (body.containsKey("key")) return Optional.ofNullable(body.get("key"));
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

    private ResponseEntity<ApiResponse<KeyValue>> okKV(KeyValue kv) {
        return ResponseEntity.ok(ApiResponse.success(200, kv));
    }
}
