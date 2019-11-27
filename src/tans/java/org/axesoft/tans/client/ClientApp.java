/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.axesoft.tans.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.netty.util.concurrent.Future;
import org.apache.commons.lang3.time.StopWatch;
import org.axesoft.jaxos.base.LongRange;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ClientApp {

    public static class Args {
        @Parameter(names = {"-s"}, description = "server addresses in syntax of 'addr1:port1;addr2:port2'")
        private String servers = "localhost:8081;localhost:8082;localhost:8083";

        @Parameter(names = {"-c"}, description = "Num of clients")
        private Integer clientNumber = 1;

        @Parameter(names = {"-k"}, description = "How many round of 1000")
        private Integer round = 1;

        @Parameter(names = {"-e"}, description = "Print every result or not")
        private Boolean printEveryResult = false;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("node-id", "client");

        Args arg = new Args();

        JCommander.newBuilder()
                .addObject(arg)
                .build()
                .parse(args);

        int n = 1000;

        int p = arg.clientNumber;

        final CountDownLatch startLatch = new CountDownLatch(1);

        final CountDownLatch endLatch = new CountDownLatch(p);

        final AtomicLong millis = new AtomicLong(0);
        final AtomicLong times = new AtomicLong(0);

        final ResultChecker checker = new ResultChecker((p * 3) / 2);

        for (int j = 0; j < p; j++) {
            new Thread(() -> {
                try {
                    run(arg.servers, arg.round, n, arg.printEveryResult, startLatch, checker, millis, times);
                }
                catch (Exception e) {
                    System.out.println("Exec end with " + e.getMessage());
                }
                finally {
                    endLatch.countDown();
                }
            }, "Worker-" + j).start();
        }

        Thread.sleep(5000);
        System.out.println("Start " + new Date());
        startLatch.countDown();

        StopWatch watch = StopWatch.createStarted();

        endLatch.await();
        System.out.println("End " + new Date());

        watch.stop();
        double sec = watch.getTime(TimeUnit.MILLISECONDS) / 1000.0;

        System.out.println(String.format("Total finish %d in %.2f seconds, OPS is %.1f",
                times.get(), sec, times.get() / sec));

        checker.printCheckResult();
    }

    public static void run(String servers, int k, int n, boolean printEveryResult,
                           CountDownLatch startLatch, ResultChecker checker,
                           AtomicLong millis, AtomicLong times) throws Exception {
        TansClientBootstrap cb = new TansClientBootstrap(servers);
        final TansClient client = cb.getClient();
        final int printInterval =  200 + (int) (Math.random() * 100);
        Thread.sleep(1000);

        startLatch.await();

        StopWatch watch = StopWatch.createStarted();
        int count = 0;
        try {
            for (int m = 0; m < k; m++) {
                boolean ignoreLeader = false; //k * 1.0 / m > Math.random();
                for (int i = 0; i < n; i++) {
                    count++;
                    String key = "object-id-" + (count % 5000);
                    Future<LongRange> future = client.acquire(key, 1 + (i % 10), ignoreLeader);
                    LongRange r = future.get();
                    checker.accept(key, r.low(), r.high());
                    if (count % printInterval == 0 || printEveryResult) {
                        System.out.println(String.format("[%s], %d, %s, %s", Thread.currentThread().getName(), count, key, r.toString()));
                    }
                }
            }
        }
        finally {
            watch.stop();
            long m = watch.getTime(TimeUnit.MILLISECONDS);
            double sec = m / 1000.0;

            millis.addAndGet(m);
            times.addAndGet(count);
            cb.close();

            System.out.println(String.format("%s finish %d in %.2f seconds, OPS is %.1f, DUR is %.1f ms",
                    Thread.currentThread().getName(), count, sec, count / sec, ((double) m) / count));
        }
    }

    public static class ResultChecker {
        private int limit;
        ConcurrentMap<String, RangeCombinator> combinatorMap = new ConcurrentHashMap<>();

        public ResultChecker(int limit) {
            this.limit = limit;
        }

        public void accept(String key, long low, long high) {
            RangeCombinator combinator = this.combinatorMap.computeIfAbsent(key, k -> new RangeCombinator(k, this.limit));
            combinator.accept(low, high);
        }

        public void printCheckResult() {
            List<String> keys = new ArrayList<>(combinatorMap.keySet());
            keys.sort(String::compareTo);

            boolean errorOccur = false;
            for (String key : keys) {
                RangeCombinator c = combinatorMap.get(key);
                if (c.getErrMsg() != null) {
                    errorOccur = true;
                    System.out.println(key + ": " + c.getErrMsg());
                }
            }

            if (!errorOccur) {
                System.out.println("All " + keys.size() + " keys are ok");
            }
        }
    }

    public static class RangeCombinator {
        private String key;
        private int limit;
        private long last = 0;
        private String errMsg;
        private SortedSet<LongRange> ranges = new TreeSet<>();

        public RangeCombinator(String key, int limit) {
            this.limit = limit;
            this.key = key;
        }

        public synchronized void accept(long low, long high) {
            if (errMsg != null) {
                return;
            }

            int size = ranges.size();
            ranges.add(new LongRange(low, high));
            if (ranges.size() == size) {
                errMsg = String.format("Overlapped [%d, %d]", low, high);
                return;
            }

            if (ranges.size() > this.limit) {
                tryConcat(this.limit + 1);
            }
        }

        public String getErrMsg() {
            if (errMsg == null) {
                tryConcat(0);
            }

            return this.errMsg;
        }

        private void tryConcat(int allowRemain) {
            while (!this.ranges.isEmpty()) {
                LongRange r0 = ranges.first();

                if (last == 0 || r0.low() == last + 1) {
                    last = r0.high();
                    ranges.remove(r0);
                }
                else {
                    if (r0.low() <= last) {
                        errMsg = String.format("Overlapped [%d, %d]", r0.low(), r0.high());
                        break;
                    }
                    if (r0.low() > last + 1) {
                        if (ranges.size() > allowRemain) {
                            errMsg = String.format("gap between %d, %s", last, r0.toString());
                        }
                        break;
                    }
                }
            }
        }
    }
}
