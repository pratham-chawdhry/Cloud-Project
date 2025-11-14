package com.worker.service;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

@Service
public class FailureReporter {

    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${controller.url}")
    private String controllerUrl;

    @Autowired
    private Environment env;

    private String workerUrl;

    @EventListener
    public void onWebServerReady(WebServerInitializedEvent event) {
        try {
            int port = event.getWebServer().getPort();
            String host = InetAddress.getLocalHost().getHostAddress();
            this.workerUrl = "http://" + host + ":" + port;
            System.out.println("Worker URL: " + workerUrl);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            this.workerUrl = "http://localhost:" + event.getWebServer().getPort();
            System.out.println("Worker URL (fallback): " + workerUrl);
        }
    }


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
