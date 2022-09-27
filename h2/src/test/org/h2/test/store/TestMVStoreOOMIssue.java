/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import org.h2.test.TestBase;

/**
 * Tests the MVStore OOM issue.
 */
public class TestMVStoreOOMIssue extends TestBase {

    protected boolean tested;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.test();
    }

    @Override
    public void test() throws Exception {
        boolean big = config.big;
        if (this.tested && !big) {
            String tag = TestMVStoreOOMIssue.class.getName();
            trace(tag +" tested then skip");
            return;
        }

        int rows = 100_000_000; // default 100 million rows
        int heapMb = 64;
        if (big) {
            rows *= 10;         // big case 1  billion rows
            heapMb += 20;       // about 2M per 10k chunks
        }
        println("Rows " + rows + " and Xmx " + heapMb + "M");
        String[] command = {
                "java", "-Xmx"+ heapMb +"m", "-Dn="+ rows, "-Dop=i",
                "-Dnull-value=true", "-Dcache-size=0",
                MVStoreOOMIssue.class.getName()
        };
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.environment().put("CLASSPATH", getClassPath());
        pb.inheritIO();
        Process process = pb.start();
        int exitValue = process.waitFor();
        assertTrue(exitValue == 0);
        this.tested = true;
    }

}
