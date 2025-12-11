package com.worker.controller;

import com.worker.model.ApiResponse;
import com.worker.model.KeyValue;
import com.worker.service.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

@RestController
@RequestMapping("/")
public class StatusController {

    @Autowired
    private KeyValueStore keyValueStore;

    @GetMapping("/status")
    public ResponseEntity<ApiResponse<Map<String, KeyValue>>> getStatus() {
        try {
            Map<String, KeyValue> data = keyValueStore.getAll();
            return ResponseEntity.ok(ApiResponse.success(200, data));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }
}
