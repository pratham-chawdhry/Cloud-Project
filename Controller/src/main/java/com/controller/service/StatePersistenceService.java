package com.controller.service;

import com.controller.model.WorkerNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service for persisting and recovering controller state to/from disk.
 * This enables the controller to recover its state after a failure/restart.
 */
@Service
public class StatePersistenceService {

    @Value("${controller.state.file:controller-state.json}")
    private String stateFileName;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private Path stateFilePath;

    @PostConstruct
    public void init() {
        // Create state file path in the current directory or temp directory
        String baseDir = System.getProperty("user.dir");
        this.stateFilePath = Paths.get(baseDir, stateFileName);
        
        // Ensure directory exists
        try {
            Files.createDirectories(stateFilePath.getParent());
        } catch (IOException e) {
            System.err.println("Warning: Could not create state file directory: " + e.getMessage());
        }
        
        System.out.println("State persistence file: " + stateFilePath.toAbsolutePath());
    }

    /**
     * Saves the current controller state to disk.
     * 
     * @param workers Map of worker URLs to WorkerNode objects
     * @param workerUrls List of worker URLs in order
     */
    public void saveState(Map<String, WorkerNode> workers, List<String> workerUrls) {
        try {
            Map<String, Object> state = new HashMap<>();
            
            // Convert WorkerNode objects to serializable format
            List<Map<String, Object>> workerNodesData = new ArrayList<>();
            for (WorkerNode worker : workers.values()) {
                Map<String, Object> workerData = new HashMap<>();
                workerData.put("url", worker.getUrl());
                workerData.put("active", worker.isActive());
                workerData.put("lastHeartbeat", worker.getLastHeartbeat() != null ? 
                    worker.getLastHeartbeat().toString() : null);
                workerData.put("replicaUrls", worker.getReplicaUrls());
                workerData.put("workerId", worker.getWorkerId());
                workerNodesData.add(workerData);
            }
            
            state.put("workers", workerNodesData);
            state.put("workerUrls", workerUrls);
            state.put("lastSaved", Instant.now().toString());
            
            // Write to file atomically
            File tempFile = new File(stateFilePath.toAbsolutePath() + ".tmp");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tempFile, state);
            
            // Atomic rename
            File stateFile = stateFilePath.toFile();
            if (tempFile.renameTo(stateFile)) {
                System.out.println("State saved successfully to " + stateFilePath);
            } else {
                // Fallback: try direct write
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(stateFile, state);
                System.out.println("State saved successfully (fallback) to " + stateFilePath);
            }
            
        } catch (IOException e) {
            System.err.println("Error saving controller state: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Loads controller state from disk.
     * 
     * @return ControllerState object containing workers and workerUrls, or null if no state exists
     */
    public ControllerState loadState() {
        File stateFile = stateFilePath.toFile();
        
        if (!stateFile.exists()) {
            System.out.println("No existing state file found at " + stateFilePath);
            return null;
        }
        
        try {
            Map<String, Object> state = objectMapper.readValue(stateFile, Map.class);
            
            // Reconstruct worker nodes
            Map<String, WorkerNode> workers = new HashMap<>();
            List<String> workerUrls = new ArrayList<>();
            
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> workersData = (List<Map<String, Object>>) state.get("workers");
            
            if (workersData != null) {
                for (Map<String, Object> workerData : workersData) {
                    String url = (String) workerData.get("url");
                    Integer workerId = (Integer) workerData.get("workerId");
                    Boolean active = (Boolean) workerData.get("active");
                    String lastHeartbeatStr = (String) workerData.get("lastHeartbeat");
                    @SuppressWarnings("unchecked")
                    List<String> replicaUrls = (List<String>) workerData.get("replicaUrls");
                    
                    WorkerNode worker = new WorkerNode(url, workerId != null ? workerId : 0);
                    worker.setActive(active != null ? active : false);
                    
                    if (lastHeartbeatStr != null) {
                        try {
                            worker.setLastHeartbeat(Instant.parse(lastHeartbeatStr));
                        } catch (Exception e) {
                            System.err.println("Error parsing lastHeartbeat for " + url + ": " + e.getMessage());
                        }
                    }
                    
                    if (replicaUrls != null) {
                        worker.setReplicaUrls(replicaUrls);
                    }
                    
                    workers.put(url, worker);
                }
            }
            
            @SuppressWarnings("unchecked")
            List<String> savedWorkerUrls = (List<String>) state.get("workerUrls");
            if (savedWorkerUrls != null) {
                workerUrls.addAll(savedWorkerUrls);
            }
            
            String lastSaved = (String) state.get("lastSaved");
            System.out.println("State loaded successfully from " + stateFilePath + 
                (lastSaved != null ? " (last saved: " + lastSaved + ")" : ""));
            
            return new ControllerState(workers, workerUrls);
            
        } catch (IOException e) {
            System.err.println("Error loading controller state: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Deletes the state file (useful for testing or manual reset).
     */
    public void deleteState() {
        try {
            Files.deleteIfExists(stateFilePath);
            System.out.println("State file deleted: " + stateFilePath);
        } catch (IOException e) {
            System.err.println("Error deleting state file: " + e.getMessage());
        }
    }

    /**
     * Container class for controller state.
     */
    public static class ControllerState {
        private final Map<String, WorkerNode> workers;
        private final List<String> workerUrls;

        public ControllerState(Map<String, WorkerNode> workers, List<String> workerUrls) {
            this.workers = workers;
            this.workerUrls = workerUrls;
        }

        public Map<String, WorkerNode> getWorkers() {
            return workers;
        }

        public List<String> getWorkerUrls() {
            return workerUrls;
        }
    }
}

