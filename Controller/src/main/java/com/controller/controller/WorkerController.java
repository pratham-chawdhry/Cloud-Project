package com.controller.controller;

import com.controller.model.ApiResponse;
import com.controller.model.WorkerNode;
import com.controller.service.ClusterResyncService;
import com.controller.service.WorkerManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/worker")
public class WorkerController {

    @Autowired
    private WorkerManager workerManager;

    @Autowired
    private ClusterResyncService clusterResyncService;

    @PostMapping("/register")
    public ResponseEntity<ApiResponse<Map<String, String>>> registerWorker(@RequestBody Map<String, String> body) {
        String url = body.get("url");
        if (url == null || url.isBlank()) return ResponseEntity.badRequest().body(ApiResponse.fail(400, "url is required"));
        String workerId = workerManager.register(url);
        Map<String, String> payload = Map.of("workerId", workerId);
        return ResponseEntity.ok(ApiResponse.success(200, payload));
    }

    @GetMapping("/health")
    public ResponseEntity<ApiResponse<Map<String, Object>>> health() {
        Map<String, Object> status = new HashMap<>();
        status.put("status", "healthy");
        status.put("activeWorkers", workerManager.getActiveWorkers().size());
        status.put("totalWorkers", workerManager.getAllWorkers().size());
        return ResponseEntity.ok(ApiResponse.success(200, status));
    }

    @GetMapping("/status")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("activeWorkers", workerManager.getActiveWorkers());
        status.put("allWorkers", workerManager.getAllWorkers());
        status.put("workerNodes", workerManager.getWorkerNodes());
        return ResponseEntity.ok(ApiResponse.success(200, status));
    }
}
