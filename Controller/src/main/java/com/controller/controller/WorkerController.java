package com.controller.controller;

import com.controller.model.ApiResponse;
import com.controller.model.WorkerNode;
import com.controller.service.ClusterResyncService;
import com.controller.service.WorkerManager;
import jakarta.servlet.http.HttpServletRequest;
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
        if (url == null || url.isBlank()) {
            return ResponseEntity.badRequest()
                    .body(ApiResponse.fail(400, "url is required"));
        }

        String workerId = workerManager.registerNewWorker(url);
        clusterResyncService.resyncCluster();

        Map<String, String> payload = new HashMap<>();
        payload.put("workerId", workerId);

        return ResponseEntity.ok(ApiResponse.success(200, payload));
    }

    @GetMapping("/health")
    public ResponseEntity<ApiResponse<Map<String, Object>>> health() {
        try {
            Map<String, Object> status = new HashMap<>();
            status.put("status", "healthy");
            status.put("activeWorkers", workerManager.getActiveWorkers().size());
            status.put("totalWorkers", workerManager.getAllWorkers().size());
            return ResponseEntity.ok(ApiResponse.success(200, status));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    @GetMapping("/status")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getStatus() {
        try {
            Map<String, Object> status = new HashMap<>();
            List<String> activeWorkers = workerManager.getActiveWorkers();
            List<String> allWorkers = workerManager.getAllWorkers();
            Collection<WorkerNode> workerNodes = workerManager.getAllWorkerNodes();

            status.put("activeWorkers", activeWorkers);
            status.put("allWorkers", allWorkers);
            status.put("workerNodes", workerNodes);

            return ResponseEntity.ok(ApiResponse.success(200, status));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    @PostMapping("/heartbeat")
    public ResponseEntity<ApiResponse<String>> heartbeat(@RequestBody Map<String, String> body) {
        try {
            String workerUrl = body.get("workerUrl");

            if (workerUrl == null) {
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "workerUrl is required"));
            }

            workerManager.updateHeartbeat(workerUrl);
            return ResponseEntity.ok(ApiResponse.success(200, "Heartbeat received"));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }
}
