package com.flo.app.datastore;

import com.flo.app.data.Nmi300;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/*
 * below class was created to
 *
 * */
public class InMemoryDataStore {
    private static final Map<String, Nmi300> nmi300 = new ConcurrentHashMap<>();

    public static final void addNmi300(Nmi300 nmiObj) {
        nmi300.put(nmiObj.getId(), nmiObj);
    }

    public static final Nmi300 getNmi300(String id) {
        return nmi300.get(id);
    }
}
