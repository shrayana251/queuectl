package com.queuectl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class Job {
    public String id;
    public String command;
    public String state; // pending, processing, completed, failed, dead
    public int attempts;
    @JsonProperty("max_retries")
    public int maxRetries;
    @JsonProperty("created_at")
    public String createdAt;
    @JsonProperty("updated_at")
    public String updatedAt;
    @JsonProperty("available_at")
    public String availableAt; // ISO-8601

    public Job() {}

    public Job(String id, String command, int maxRetries) {
        this.id = id;
        this.command = command;
        this.state = "pending";
        this.attempts = 0;
        this.maxRetries = maxRetries;
        this.createdAt = Instant.now().toString();
        this.updatedAt = this.createdAt;
        this.availableAt = this.createdAt;
    }

    @JsonIgnore
    public boolean isRetryable() {
        return attempts < maxRetries;
    }
}
