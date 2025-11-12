package com.worker.controller;

import com.worker.model.ApiResponse;
import com.worker.model.ReplicaAssignment;
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
    public ResponseEntity<ApiResponse<String>> configureReplicas(@RequestBody ReplicaAssignment assignment) {
        try {
            replicationService.updateReplicaTargets(assignment.getSyncReplica(), assignment.getAsyncReplica());
            return ResponseEntity.ok(ApiResponse.success(200, "Replica targets updated"));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    @GetMapping("/replicas")
    public ResponseEntity<ApiResponse<ReplicaAssignment>> getReplicas() {
        try {
            var assignment = new ReplicaAssignment();
            assignment.setSyncReplica(replicationService.getSyncReplica());
            assignment.setAsyncReplica(replicationService.getAsyncReplica());
            return ResponseEntity.ok(ApiResponse.success(200, assignment));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(ApiResponse.fail(500, e.getMessage()));
        }
    }
}
