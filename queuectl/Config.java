package com.queuectl;

public class Config {
    // Default values
    private int maxRetries = 3;
    private double backoffBase = 2.0; // delay = base ^ attempts (seconds)

    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    public double getBackoffBase() { return backoffBase; }
    public void setBackoffBase(double backoffBase) { this.backoffBase = backoffBase; }
}
