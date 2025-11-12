package com.worker.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
public class FailureReporter {

    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${controller.url}")
    private String controllerUrl;

    @Value("${worker.url}")
    private String workerUrl;

    public void reportSyncFailure(String failedTarget, String reason) {
        try {
            restTemplate.postForEntity(
                    controllerUrl + "/controller/reportFailure",
                    Map.of(
                            "reporter", workerUrl,
                            "failedTarget", failedTarget,
                            "reason", reason
                    ),
                    String.class
            );

            System.out.println("Reported sync failure to controller: " + failedTarget);

        } catch (Exception ignored) {
            System.err.println("Failed to report sync failure to controller");
        }
    }
}
