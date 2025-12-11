package com.worker.controller;

import com.worker.model.ApiResponse;
import com.worker.service.FailoverService;
import com.worker.service.RecoveryService;
import com.worker.service.ReplicationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/replicas")
public class ReplicaController {

    private final ReplicationService replicationService;

    @Autowired
    public FailoverService failoverService;

    @Autowired
    public RecoveryService recoveryService;

    @Autowired
    public ReplicaController(ReplicationService replicationService) {
        this.replicationService = replicationService;
    }

    @PostMapping("/update")
    public ResponseEntity<ApiResponse<String>> configureReplicas(
            @RequestBody Map<String, Object> body) {

        try {
            List<String> deadWorkers  = (List<String>) body.get("deadWorkers");
            List<String> aliveWorkers = (List<String>) body.get("aliveWorkers");

            replicationService.updateClusterState(new HashSet<>(aliveWorkers));

            failoverService.applyFailover(deadWorkers);
            recoveryService.applyRecovery();

            return ResponseEntity.ok(
                    ApiResponse.success(200, "Cluster state updated")
            );

        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }
}
