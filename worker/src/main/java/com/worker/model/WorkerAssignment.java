package com.worker.model;

import lombok.Data;

@Data
public class ReplicaAssignment {
    private String syncReplica;
    private String asyncReplica;
}
