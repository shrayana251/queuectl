package com.queuectl;

import java.util.ArrayList;
import java.util.List;

public class WorkerManager {
    private final List<Thread> threads = new ArrayList<>();
    private final List<Worker> workers = new ArrayList<>();
    private final JobStore store;
    private final Config cfg;

    public WorkerManager(JobStore store, Config cfg) {
        this.store = store;
        this.cfg = cfg;
    }

    public synchronized void start(int count) {
        stop(); // stop existing
        for (int i = 0; i < count; i++) {
            Worker w = new Worker("w-" + i, store, cfg);
            Thread t = new Thread(w);
            t.setName("worker-" + i);
            threads.add(t);
            workers.add(w);
            t.start();
        }
    }

    public synchronized void stop() {
        for (Worker w : workers) w.shutdown();
        for (Thread t : threads) t.interrupt();
        for (Thread t : threads) {
            try { t.join(2000); } catch (InterruptedException ignored) {}
        }
        threads.clear();
        workers.clear();
    }

    public synchronized int activeCount() {
        return threads.size();
    }
}
