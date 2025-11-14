package com.worker.model;

import lombok.Data;

@Data
public class WorkerAssignment {
    private String syncReplica;
    private String asyncReplica;
    private String controllerUrl;
}
