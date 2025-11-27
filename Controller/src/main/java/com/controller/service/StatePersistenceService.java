package com.controller.service;

import com.controller.model.WorkerNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
public class StatePersistenceService {

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
            .enable(SerializationFeature.INDENT_OUTPUT);

    @Value("${controller.state.file:controller-state.json}")
    private String stateFilePath;

    @Value("${controller.state.fallback-dir:./}")
    private String fallbackDir;

    public static class ControllerState {
        private Map<String, WorkerNode> workers;
        private List<String> workerUrls;
        private long lastSaved;

        public ControllerState() {}

        public ControllerState(Map<String, WorkerNode> workers, List<String> workerUrls) {
            this.workers = workers;
            this.workerUrls = workerUrls;
            this.lastSaved = System.currentTimeMillis();
        }

        public Map<String, WorkerNode> getWorkers() { return workers; }
        public List<String> getWorkerUrls() { return workerUrls; }
        public long getLastSaved() { return lastSaved; }
        public void setWorkers(Map<String, WorkerNode> workers) { this.workers = workers; }
        public void setWorkerUrls(List<String> workerUrls) { this.workerUrls = workerUrls; }
        public void setLastSaved(long lastSaved) { this.lastSaved = lastSaved; }
    }

    public synchronized void saveState(Map<String, WorkerNode> workers, List<String> workerUrls) {
        ControllerState state = new ControllerState(workers, workerUrls);
        File f = new File(stateFilePath);
        try {
            // ensure parent
            if (f.getParentFile() != null) f.getParentFile().mkdirs();
            mapper.writeValue(f, state);
            System.out.println("State saved to " + f.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("Failed to save state to " + f.getAbsolutePath() + ": " + e.getMessage());
            // fallback: try to save to fallbackDir with timestamp
            try {
                Files.createDirectories(new File(fallbackDir).toPath());
                File fallback = new File(fallbackDir, "controller-state-fallback-" + System.currentTimeMillis() + ".json");
                mapper.writeValue(fallback, state);
                System.out.println("State saved successfully (fallback) to " + fallback.getAbsolutePath());
            } catch (IOException ex) {
                System.err.println("Fallback save also failed: " + ex.getMessage());
            }
        }
    }

    public synchronized ControllerState loadState() {
        File f = new File(stateFilePath);
        if (!f.exists()) {
            System.out.println("No state file at " + f.getAbsolutePath());
            return null;
        }
        try {
            ControllerState s = mapper.readValue(f, ControllerState.class);
            // Defensive: ensure non-null collections
            if (s.getWorkers() == null) s.setWorkers(Collections.emptyMap());
            if (s.getWorkerUrls() == null) s.setWorkerUrls(Collections.emptyList());
            System.out.println("State loaded from " + f.getAbsolutePath() + ", workers=" + s.getWorkers().size());
            return s;
        } catch (IOException e) {
            System.err.println("Failed to read state file: " + e.getMessage());
            return null;
        }
    }
}
