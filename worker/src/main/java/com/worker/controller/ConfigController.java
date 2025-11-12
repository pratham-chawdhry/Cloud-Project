package com.worker.controller;

import com.worker.model.ApiResponse;
import com.worker.service.ReplicationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/config")
public class ConfigController {

    private final ReplicationService replicationService;

    @Autowired
    public ConfigController(ReplicationService replicationService) {
        this.replicationService = replicationService;
    }

    @PostMapping("/replicas")
    public ResponseEntity<ApiResponse<String>> configureReplicas(@RequestBody List<String> newReplicas) {
        try {
            replicationService.setReplicas(newReplicas);
            System.out.println("Worker replica list updated: " + newReplicas);
            return ResponseEntity.ok(ApiResponse.success(200, "Replica configuration updated successfully"));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    @GetMapping("/replicas")
    public ResponseEntity<ApiResponse<List<String>>> getReplicas() {
        try {
            List<String> replicas = replicationService.getReplicas();
            return ResponseEntity.ok(ApiResponse.success(200, replicas));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(ApiResponse.fail(500, e.getMessage()));
        }
    }
}
