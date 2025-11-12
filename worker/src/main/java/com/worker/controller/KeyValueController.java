package com.worker.controller;

import com.worker.model.ApiResponse;
import com.worker.model.KeyValue;
import com.worker.service.KeyValueStore;
import com.worker.service.ReplicationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

@RestController
@RequestMapping("/")
public class KeyValueController {

    @Autowired
    private KeyValueStore keyValueStore;

    @Autowired
    private ReplicationService replicationService;

    @PutMapping("/put")
    public ResponseEntity<ApiResponse<String>> put(@RequestBody Map<String, String> body) {
        try {
            String key = body.get("key");
            String value = body.get("value");
            if (key == null || value == null)
                return ResponseEntity.badRequest().body(ApiResponse.fail(400, "Key and value required"));
            keyValueStore.put(key, value);
            new Thread(() -> replicationService.replicateToReplicas(key, value)).start();
            return ResponseEntity.ok(ApiResponse.success(200, "Stored key=" + key));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    @PostMapping("/replicate")
    public ResponseEntity<ApiResponse<String>> replicate(@RequestBody Map<String, String> body) {
        try {
            String key = body.get("key");
            String value = body.get("value");
            if (key == null || value == null)
                return ResponseEntity.badRequest().body(ApiResponse.fail(400, "Key and value required"));
            keyValueStore.put(key, value);
            return ResponseEntity.ok(ApiResponse.success(200, "Replicated key=" + key));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    @PostMapping("/get")
    public ResponseEntity<ApiResponse<KeyValue>> get(@RequestBody Map<String, String> body) {
        try {
            String key = body.get("key");
            if (key == null)
                return ResponseEntity.badRequest().body(ApiResponse.fail(400, "Key required"));
            String value = keyValueStore.get(key);
            if (value == null)
                return ResponseEntity.status(404).body(ApiResponse.fail(404, "Key not found"));
            return ResponseEntity.ok(ApiResponse.success(200, new KeyValue(key, value)));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(ApiResponse.fail(500, e.getMessage()));
        }
    }
}
