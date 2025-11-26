package com.controller.controller;

import com.controller.model.ApiResponse;
import com.controller.model.WorkerNode;
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

    /**
     * Controller-level health
     */
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

    /**
     * Full worker registry snapshot
     */
    @GetMapping("/status")
    public ResponseEntity<ApiResponse<Map<String, Object>>> status() {
        try {
            Map<String, Object> resp = new HashMap<>();

            resp.put("activeWorkers", workerManager.getActiveWorkers());
            resp.put("allWorkers", workerManager.getAllWorkers());
            resp.put("workerNodes", workerManager.getAllWorkerNodes());

            return ResponseEntity.ok(ApiResponse.success(200, resp));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    /**
     * Identify the primary owner for a key
     */
    @PostMapping("/whoami")
    public ResponseEntity<ApiResponse<Map<String, Object>>> whoami(@RequestBody Map<String, String> body) {
        try {
            String key = body.get("key");
            if (key == null || key.trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "Key is required"));
            }

            String worker = workerManager.getWorkerForKey(key);

            Map<String, Object> resp = new HashMap<>();
            resp.put("key", key);
            resp.put("worker", worker);

            return ResponseEntity.ok(ApiResponse.success(200, resp));

        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    /**
     * Debug: Force mark a worker DOWN
     */
    @PostMapping("/markFailed")
    public ResponseEntity<ApiResponse<String>> markFailed(@RequestBody Map<String, String> body) {
        try {
            String url = body.get("workerUrl");
            if (url == null || url.trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "workerUrl is required"));
            }

            workerManager.markWorkerAsFailed(url);
            return ResponseEntity.ok(ApiResponse.success(200, "Marked as failed: " + url));

        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    /**
     * Debug: Force mark a worker UP
     */
    @PostMapping("/markRecovered")
    public ResponseEntity<ApiResponse<String>> markRecovered(@RequestBody Map<String, String> body) {
        try {
            String url = body.get("workerUrl");
            if (url == null || url.trim().isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "workerUrl is required"));
            }

            workerManager.markWorkerAsRecovered(url);
            return ResponseEntity.ok(ApiResponse.success(200, "Marked as recovered: " + url));

        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }
}
