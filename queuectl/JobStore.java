package com.queuectl;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class JobStore {
    private final Connection conn;
    private final ObjectMapper mapper = new ObjectMapper();

    public JobStore(String dbPath) throws SQLException {
        String url = "jdbc:sqlite:" + dbPath;
        conn = DriverManager.getConnection(url);
        init();
    }

    private void init() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                  id TEXT PRIMARY KEY,
                  command TEXT NOT NULL,
                  state TEXT NOT NULL,
                  attempts INTEGER NOT NULL,
                  max_retries INTEGER NOT NULL,
                  created_at TEXT,
                  updated_at TEXT,
                  available_at TEXT
                );
            """);
        }
    }

    public synchronized void enqueue(Job job) throws SQLException {
        String sql = "INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, available_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement p = conn.prepareStatement(sql)) {
            p.setString(1, job.id);
            p.setString(2, job.command);
            p.setString(3, job.state);
            p.setInt(4, job.attempts);
            p.setInt(5, job.maxRetries);
            p.setString(6, job.createdAt);
            p.setString(7, job.updatedAt);
            p.setString(8, job.availableAt);
            p.executeUpdate();
        }
    }

    public synchronized boolean markProcessingAndReserveNext(Config cfg, JobHolder out) throws SQLException {
        // We atomically reserve a pending job by updating it; update returns rows updated.
        // We pick the oldest pending job whose available_at <= now
        String selectId = "SELECT id FROM jobs WHERE state = 'pending' AND datetime(available_at) <= datetime('now') ORDER BY created_at LIMIT 1";
        conn.setAutoCommit(false);
        try (PreparedStatement sel = conn.prepareStatement(selectId);
             ResultSet rs = sel.executeQuery()) {
            if (!rs.next()) {
                conn.commit();
                conn.setAutoCommit(true);
                return false;
            }
            String id = rs.getString("id");
            // Update to processing and increment attempts
            String upd = "UPDATE jobs SET state='processing', attempts = attempts + 1, updated_at = ?, available_at = ? WHERE id = ? AND state = 'pending'";
            String now = Instant.now().toString();
            // tentative backoff placeholder: set available_at far future to avoid double pick (we'll set correct on failure)
            String reservedUntil = Instant.now().plus(365, ChronoUnit.DAYS).toString();
            try (PreparedStatement p = conn.prepareStatement(upd)) {
                p.setString(1, now);
                p.setString(2, reservedUntil);
                p.setString(3, id);
                int updated = p.executeUpdate();
                if (updated == 0) {
                    conn.rollback();
                    conn.setAutoCommit(true);
                    return false;
                }
            }

            // retrieve full row
            String getRow = "SELECT * FROM jobs WHERE id = ?";
            try (PreparedStatement g = conn.prepareStatement(getRow)) {
                g.setString(1, id);
                try (ResultSet r2 = g.executeQuery()) {
                    if (r2.next()) {
                        Job j = readJobFromResult(r2);
                        conn.commit();
                        conn.setAutoCommit(true);
                        out.job = j;
                        return true;
                    }
                }
            }
            conn.rollback();
            conn.setAutoCommit(true);
            return false;
        } catch (SQLException ex) {
            conn.rollback();
            conn.setAutoCommit(true);
            throw ex;
        }
    }

    private Job readJobFromResult(ResultSet r) throws SQLException {
        Job j = new Job();
        j.id = r.getString("id");
        j.command = r.getString("command");
        j.state = r.getString("state");
        j.attempts = r.getInt("attempts");
        j.maxRetries = r.getInt("max_retries");
        j.createdAt = r.getString("created_at");
        j.updatedAt = r.getString("updated_at");
        j.availableAt = r.getString("available_at");
        return j;
    }

    public synchronized void completeJob(String id) throws SQLException {
        String sql = "UPDATE jobs SET state='completed', updated_at = ? WHERE id = ?";
        try (PreparedStatement p = conn.prepareStatement(sql)) {
            p.setString(1, Instant.now().toString());
            p.setString(2, id);
            p.executeUpdate();
        }
    }

    public synchronized void failAndScheduleRetry(Job job, Config cfg) throws SQLException {
        int attempts = job.attempts; // note: attempts was incremented when reserved
        if (attempts > job.maxRetries) {
            // move to dead
            String sql = "UPDATE jobs SET state='dead', updated_at = ? WHERE id = ?";
            try (PreparedStatement p = conn.prepareStatement(sql)) {
                p.setString(1, Instant.now().toString());
                p.setString(2, job.id);
                p.executeUpdate();
            }
            return;
        }
        // compute delay = base ^ attempts (seconds)
        double delaySec = Math.pow(cfg.getBackoffBase(), attempts);
        Instant next = Instant.now().plus((long)Math.ceil(delaySec), java.time.temporal.ChronoUnit.SECONDS);
        String sql = "UPDATE jobs SET state='pending', updated_at = ?, available_at = ? WHERE id = ?";
        try (PreparedStatement p = conn.prepareStatement(sql)) {
            p.setString(1, Instant.now().toString());
            p.setString(2, next.toString());
            p.setString(3, job.id);
            p.executeUpdate();
        }
    }

    public synchronized List<Job> listByState(String state) throws SQLException {
        List<Job> out = new ArrayList<>();
        String sql = "SELECT * FROM jobs WHERE state = ? ORDER BY created_at";
        try (PreparedStatement p = conn.prepareStatement(sql)) {
            p.setString(1, state);
            try (ResultSet r = p.executeQuery()) {
                while (r.next()) out.add(readJobFromResult(r));
            }
        }
        return out;
    }

    public synchronized List<Job> listAll() throws SQLException {
        List<Job> out = new ArrayList<>();
        String sql = "SELECT * FROM jobs ORDER BY created_at";
        try (PreparedStatement p = conn.prepareStatement(sql);
             ResultSet r = p.executeQuery()) {
            while (r.next()) out.add(readJobFromResult(r));
        }
        return out;
    }

    public synchronized Job getJob(String id) throws SQLException {
        String sql = "SELECT * FROM jobs WHERE id = ?";
        try (PreparedStatement p = conn.prepareStatement(sql)) {
            p.setString(1, id);
            try (ResultSet r = p.executeQuery()) {
                if (r.next()) return readJobFromResult(r);
            }
        }
        return null;
    }

    public synchronized int countByState(String state) throws SQLException {
        String sql = "SELECT COUNT(*) AS c FROM jobs WHERE state = ?";
        try (PreparedStatement p = conn.prepareStatement(sql)) {
            p.setString(1, state);
            try (ResultSet r = p.executeQuery()) {
                if (r.next()) return r.getInt("c");
            }
        }
        return 0;
    }

    public void close() throws SQLException {
        conn.close();
    }

    public static class JobHolder { public Job job; }
}
