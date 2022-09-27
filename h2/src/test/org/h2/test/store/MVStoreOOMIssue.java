package org.h2.test.store;

import org.h2.mvstore.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.text.*;
import java.io.*;

public class MVStoreOOMIssue {

    static final int o = Integer.getInteger("o", 0);           // Offset
    static final int n = Integer.getInteger("n", 100_000_000); // Limit
    static final int p = Integer.getInteger("p", 10_000);      // Print and commit
    static final String op = System.getProperty("op", "q");    // operation
    static final boolean nul = Boolean.getBoolean("null-value");
    static final int newDelta = Integer.getInteger("new-value-delta", 0);
    static final int oldDelta = Integer.getInteger("old-value-delta", 0);
    static final int cacheSize = Integer.getInteger("cache-size", 16);
    static final int threads = Integer.getInteger("threads", 10);

    public static void main(String[] args) throws Exception {
        long s = System.currentTimeMillis();
        String mainName = Thread.currentThread().getName();
        String tempDir = System.getProperty("java.io.tmpdir");
        String fileName = String.format("%s%s%s.mv", tempDir, File.separator,
                TestMVStoreOOMIssue.class.getSimpleName());
        Path filePath = Paths.get(fileName);
        DateFormat tf = new SimpleDateFormat("HH:mm:ss.SSS");
        System.out.printf("%s[%s] open store ...%n", tf.format(new Date()), mainName);
        if (filePath.toFile().isFile()) Files.delete(filePath);

        Thread[] workers = new Thread[threads];
        int limitPerWorker = (n - o) / threads;
        MVStore store = new MVStore.Builder()
                .fileName(fileName)
                .cacheSize(cacheSize/*MB*/)
                .open();
        try {
            System.out.printf("%s[%s] open store OK%n", tf.format(new Date()), mainName);

            Map<String, Double> account = store.openMap("account");
            for (int wi = 0; wi < threads; ++wi) {
                int offset = limitPerWorker * wi;
                int limit;
                if (wi == threads - 1) limit = n;
                else limit = offset + limitPerWorker;

                workers[wi] = new Thread(() -> {
                    long ts = System.currentTimeMillis();
                    DateFormat df = new SimpleDateFormat("HH:mm:ss.SSS");
                    String thrName = Thread.currentThread().getName();
                    int i = offset;
                    try {
                        System.out.printf("%s[%s] op %s, range %d -> %d%n",
                                df.format(new Date()), thrName, op, offset, limit);
                        switch (op) {
                            case "i":
                            case "u":
                                for (; i < limit; ++i) {
                                    String name = "acc-" + i;
                                    double balance = i % 10000;
                                    // add/update the key-value pair to the store.
                                    Double old = account.put(name, balance + newDelta);
                                    if (i % p == 0) {
                                        store.commit();
                                        //System.out.printf("%s[%s] op %s, commit at %d%n",
                                        //        df.format(new Date()), thrName, op, i);
                                    }
                                    if (nul) {
                                        if (old != null) throw new AssertionError(name + "'s exists: " + old);
                                    } else if (old == null || old != balance + oldDelta) {
                                        throw new AssertionError(name + "'s balance error: old = " + old);
                                    }
                                }
                                break;
                            case "d":
                                for (; i < limit; ++i) {
                                    String name = "acc-" + i;
                                    double balance = i % 10000;
                                    Double old = account.remove(name);
                                    if (i % p == 0) {
                                        store.commit();
                                        //System.out.printf("%s[%s] op %s, commit at %d%n",
                                        //        df.format(new Date()), thrName, op, i);
                                    }
                                    if (nul) {
                                        if (old != null) throw new AssertionError(name + "'s exists: " + old);
                                    } else if (old == null || old != balance + oldDelta) {
                                        throw new AssertionError(name + "'s balance error: old = " + old);
                                    }
                                }
                                break;
                            default:
                                for (; i < limit; ++i) {
                                    String name = "acc-" + i;
                                    double balance = i % 10000;
                                    Double old = account.get(name);
                                    if (i % p == 0) {
                                        //System.out.printf("%s[%s] op %s, i %d%n",
                                        //        df.format(new Date()), thrName, op, i);
                                    }
                                    if (nul) {
                                        if (old != null) throw new AssertionError(name + "'s exists: " + old);
                                    } else if (old == null || old != balance + oldDelta) {
                                        throw new AssertionError(name + "'s balance error: old = " + old);
                                    }
                                }
                                break;
                        }
                    } finally {
                        long te = System.currentTimeMillis();
                        System.out.printf("%s[%s] op %s, null-value %s, i %d, items %d, time %dms%n",
                                df.format(new Date()), thrName, op, nul, i, n, te - ts);
                    }
                }, "worker-" + wi);
                workers[wi].start();
            } // for-threads
        } finally {
            for (int i = 0; i < threads; ++i) {
                workers[i].join();
            }
            store.close();
            Files.delete(filePath);
            long e = System.currentTimeMillis();
            System.out.printf("%s[%s] op %s, threads %s, null-value %s, items %d, time %dms%n",
                    tf.format(new Date()), mainName, op, threads, nul, n, e - s);
        }
    }

}
