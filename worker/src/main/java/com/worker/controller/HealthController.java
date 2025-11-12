package com.worker.controller;

import com.worker.model.ApiResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/")
public class HealthController {

    @GetMapping("/health")
    public ResponseEntity<ApiResponse<Map<String, Object>>> heartbeat() {
        try {
            Map<String, Object> status = new HashMap<>();
            status.put("status", "alive");
            status.put("timestamp", Instant.now().toString());
            return ResponseEntity.ok(ApiResponse.success(200, status));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(ApiResponse.fail(500, e.getMessage()));
        }
    }
}
