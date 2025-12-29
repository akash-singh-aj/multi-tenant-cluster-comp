package com.flo.app.woker;

import com.flo.app.data.Nmi300;
import com.flo.app.datastore.InMemoryDataStore;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Nmi300PersistenceWorker {
    private static final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public static void shutdown() {
        executor.shutdownNow();
    }

    public static void submitTask(Nmi300 nmi300Obj) {
        executor.submit(new Nmi300PersistenceTask(nmi300Obj));
    }

    private record Nmi300PersistenceTask(Nmi300 nmi300) implements Runnable {
        @Override
        public void run() {
            InMemoryDataStore.addNmi300(nmi300);
            System.out.println("consume from stream ");
        }
    }
}
