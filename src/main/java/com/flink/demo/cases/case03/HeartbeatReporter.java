package com.flink.demo.cases.case03;

public class HeartbeatReporter implements Runnable{

    @Override
    public void run() {
        int i = 0;
        while (true) {
            try {
                System.err.println("heartbeat " + i++);
                Thread.sleep(1000 * 5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void start() {
        System.err.println("starting job manager heartbeat reporter......");
        HeartbeatReporter reporter = new HeartbeatReporter();
        Thread heartbeatThread = new Thread(reporter, "HeartbeatTh");
        heartbeatThread.start();
    }

    public static void shutdown() {
        System.err.println("stop job manager heartbeat reporter.");
    }

}
