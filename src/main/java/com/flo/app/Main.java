package com.flo.app;

import com.flo.app.woker.Nmi300CsvReadWorker;
import com.flo.app.woker.Nmi300PersistenceWorker;

public class Main {
    static void main(String[] args) {
        initApp();
    }

    private static void initApp() {
        System.out.println(String.format("starting mt cluster"));
        // read properties and build the config
        Nmi300CsvReadWorker.start();

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            System.out.println("main " + e);
        }

        Nmi300CsvReadWorker.shutdown();
        Nmi300PersistenceWorker.shutdown();
    }
}
