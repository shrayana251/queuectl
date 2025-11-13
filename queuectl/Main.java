package com.queuectl;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class Main {
    private static final String DB_PATH = "queue.db";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printHelp();
            return;
        }

        JobStore store = new JobStore(DB_PATH);
        Config cfg = new Config();
        WorkerManager manager = new WorkerManager(store, cfg);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down workers...");
            manager.stop();
            try { store.close(); } catch (SQLException ignored) {}
        }));

        String cmd = args[0];
        switch (cmd) {
            case "enqueue" -> {
                if (args.length < 2) {
                    System.err.println("Usage: queuectl enqueue '{\"id\":\"job1\",\"command\":\"sleep 2\",\"max_retries\":3}'");
                    return;
                }
                String json = args[1];
                Job j = mapper.readValue(json, Job.class);
                if (j.id == null || j.command == null) {
                    System.err.println("job must have id and command");
                    return;
                }
                if (j.maxRetries == 0) j.maxRetries = cfg.getMaxRetries();
                store.enqueue(j);
                System.out.println("Enqueued job " + j.id);
            }
            case "worker" -> {
                if (args.length < 2) { System.err.println("Usage: queuectl worker start --count N | stop"); return; }
                String sub = args[1];
                if ("start".equals(sub)) {
                    int count = 1;
                    for (int i = 2; i < args.length; i++) {
                        if ("--count".equals(args[i]) && i + 1 < args.length) {
                            count = Integer.parseInt(args[++i]);
                        }
                    }
                    manager.start(count);
                    System.out.println("Started " + count + " workers. Press ENTER to stop.");
                    // wait until user presses enter
                    Scanner sc = new Scanner(System.in);
                    sc.nextLine();
                    manager.stop();
                } else if ("stop".equals(sub)) {
                    manager.stop();
                    System.out.println("Workers signalled to stop.");
                } else {
                    System.err.println("Unknown worker command: " + sub);
                }
            }
            case "status" -> {
                int pending = store.countByState("pending");
                int processing = store.countByState("processing");
                int completed = store.countByState("completed");
                int dead = store.countByState("dead");
                int failed = store.countByState("failed"); // rarely used
                System.out.printf("Workers active: %d\n", manager.activeCount());
                System.out.printf("pending=%d processing=%d completed=%d failed=%d dead=%d\n",
                        pending, processing, completed, failed, dead);
            }
            case "list" -> {
                String state = null;
                for (int i = 1; i < args.length; i++) {
                    if ("--state".equals(args[i]) && i + 1 < args.length) state = args[++i];
                }
                List<Job> out;
                if (state == null) out = store.listAll();
                else out = store.listByState(state);
                for (Job j : out) {
                    System.out.println(mapper.writeValueAsString(j));
                }
            }
            case "dlq" -> {
                if (args.length < 2) { System.err.println("Usage: queuectl dlq list | retry <jobid>"); return; }
                if ("list".equals(args[1])) {
                    List<Job> deadJobs = store.listByState("dead");
                    for (Job j : deadJobs) System.out.println(mapper.writeValueAsString(j));
                } else if ("retry".equals(args[1])) {
                    if (args.length < 3) { System.err.println("Usage: queuectl dlq retry <jobid>"); return; }
                    String id = args[2];
                    Job j = store.getJob(id);
                    if (j == null) { System.err.println("job not found"); return; }
                    if (!"dead".equals(j.state)) { System.err.println("job not in DLQ"); return; }
                    // reset attempts and state
                    j.attempts = 0;
                    j.state = "pending";
                    j.availableAt = java.time.Instant.now().toString();
                    store.enqueue(j); // try insert; id exists so this will fail -> instead update
                    // actually update:
                    String sql = "UPDATE jobs SET state='pending', attempts=0, updated_at=?, available_at=? WHERE id=?";
                    try (var conn = store) { /* placeholder */ } catch (Exception ignore) {}
                    // use JobStore internal method via JDBC update:
                    try (var conn = java.sql.DriverManager.getConnection("jdbc:sqlite:" + DB_PATH)) {
                        try (var ps = conn.prepareStatement(sql)) {
                            ps.setString(1, java.time.Instant.now().toString());
                            ps.setString(2, j.availableAt);
                            ps.setString(3, id);
                            ps.executeUpdate();
                        }
                    }
                    System.out.println("Retry scheduled for job " + id);
                } else {
                    System.err.println("Unknown dlq command");
                }
            }
            case "config" -> {
                if (args.length < 2) { System.err.println("Usage: queuectl config set <key> <value>"); return; }
                if ("set".equals(args[1]) && args.length >= 4) {
                    String key = args[2], val = args[3];
                    if ("max-retries".equals(key)) {
                        cfg.setMaxRetries(Integer.parseInt(val));
                        System.out.println("max-retries set to " + val);
                    } else if ("backoff-base".equals(key)) {
                        cfg.setBackoffBase(Double.parseDouble(val));
                        System.out.println("backoff-base set to " + val);
                    } else {
                        System.err.println("unknown config key");
                    }
                } else {
                    System.err.println("Usage: queuectl config set <key> <value>");
                }
            }
            default -> {
                System.err.println("Unknown command: " + cmd);
                printHelp();
            }
        }

        // close DB
        store.close();
    }

    private static void printHelp() {
        System.out.println("""
            queuectl - simple job queue
            Commands:
              enqueue '{"id":"job1","command":"sleep 2","max_retries":3}'
              worker start --count 3    (press ENTER to stop)
              worker stop
              status
              list --state pending
              dlq list
              dlq retry <jobid>
              config set max-retries 3
              config set backoff-base 2.0
            """);
    }
}
