package com.queuectl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.SQLException;

public class Worker implements Runnable {
    private final String name;
    private final JobStore store;
    private final Config cfg;
    private volatile boolean running = true;

    public Worker(String name, JobStore store, Config cfg) {
        this.name = name;
        this.store = store;
        this.cfg = cfg;
    }

    public void shutdown() {
        running = false;
    }

    @Override
    public void run() {
        System.out.println("Worker " + name + " started");
        while (running) {
            try {
                JobStore.JobHolder holder = new JobStore.JobHolder();
                boolean got = store.markProcessingAndReserveNext(cfg, holder);
                if (!got) {
                    // no job, sleep a bit
                    Thread.sleep(500);
                    continue;
                }
                Job job = holder.job;
                System.out.println("[" + name + "] picked job " + job.id + " (attempt " + job.attempts + ")");
                int exit = executeCommand(job.command);
                if (exit == 0) {
                    store.completeJob(job.id);
                    System.out.println("[" + name + "] completed job " + job.id);
                } else {
                    // on failure, schedule retry or dead
                    // refresh job from db to use updated attempts
                    Job fresh = store.getJob(job.id);
                    if (fresh != null) {
                        store.failAndScheduleRetry(fresh, cfg);
                        Job after = store.getJob(job.id);
                        if ("dead".equals(after.state)) {
                            System.out.println("[" + name + "] job moved to DLQ: " + job.id);
                        } else {
                            System.out.println("[" + name + "] job will retry at " + after.availableAt + " attempts=" + after.attempts);
                        }
                    } else {
                        System.err.println("[" + name + "] job disappeared: " + job.id);
                    }
                }
            } catch (SQLException e) {
                System.err.println("[" + name + "] SQL error: " + e.getMessage());
                try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            } catch (InterruptedException e) {
                // shutdown requested
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("[" + name + "] error: " + e.getMessage());
                try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            }
        }
        System.out.println("Worker " + name + " stopped");
    }

    private int executeCommand(String command) {
        try {
            // Execute using shell for composite commands
            String[] cmd = {"/bin/sh", "-c", command};
            ProcessBuilder pb = new ProcessBuilder(cmd);
            Process p = pb.start();

            // stream output to console
            try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
                 BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream()))) {
                String line;
                while (in.ready() && (line = in.readLine()) != null) { System.out.println("OUT: " + line); }
                while (err.ready() && (line = err.readLine()) != null) { System.err.println("ERR: " + line); }
            } catch (Exception ignore) {}

            int exit = p.waitFor();
            return exit;
        } catch (Exception e) {
            System.err.println("executeCommand error: " + e.getMessage());
            return 1;
        }
    }
}
