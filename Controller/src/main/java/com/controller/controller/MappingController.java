package com.controller.controller;

import com.controller.model.ApiResponse;
import com.controller.service.WorkerManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/mapping")
public class MappingController {

    @Autowired
    private WorkerManager workerManager;

    @GetMapping("/worker")
    public ResponseEntity<ApiResponse<Map<String, String>>> getWorkerForKey(@RequestParam String key) {
        try {
            if (key == null || key.isEmpty()) {
                return ResponseEntity.badRequest()
                    .body(ApiResponse.fail(400, "Key is required"));
            }

            String workerUrl = workerManager.getWorkerForKey(key);
            Map<String, String> mapping = new HashMap<>();
            mapping.put("key", key);
            mapping.put("worker", workerUrl);
            
            return ResponseEntity.ok(ApiResponse.success(200, mapping));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    @GetMapping("/workers")
    public ResponseEntity<ApiResponse<List<String>>> getActiveWorkers() {
        try {
            List<String> activeWorkers = workerManager.getActiveWorkers();
            return ResponseEntity.ok(ApiResponse.success(200, activeWorkers));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    @GetMapping("/all")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getAllMappings() {
        try {
            Map<String, Object> mappings = new HashMap<>();
            List<String> activeWorkers = workerManager.getActiveWorkers();
            List<String> allWorkers = workerManager.getAllWorkers();
            
            mappings.put("activeWorkers", activeWorkers);
            mappings.put("allWorkers", allWorkers);
            mappings.put("totalWorkers", allWorkers.size());
            mappings.put("activeCount", activeWorkers.size());
            
            return ResponseEntity.ok(ApiResponse.success(200, mappings));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body(ApiResponse.fail(500, e.getMessage()));
        }
    }
}
