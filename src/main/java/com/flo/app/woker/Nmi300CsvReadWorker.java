package com.flo.app.woker;

import com.flo.app.data.Nmi300;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Nmi300CsvReadWorker {

    static final ExecutorService executor = Executors.newSingleThreadExecutor();

    private static volatile boolean running = true;

    public static void start() {
        executor.execute(task);
    }

    public static void shutdown() {
        running = false;
        executor.shutdownNow();
    }

    private static Runnable task = () -> {
        while (running) {
            Nmi300 peekObj = new Nmi300();
            peekObj.setId(Double.toString(Math.random()));
            Nmi300PersistenceWorker.submitTask(peekObj);
        }
    };
}