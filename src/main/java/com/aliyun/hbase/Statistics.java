package com.aliyun.hbase;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by fengshen on 11/18/17.
 */
public class Statistics extends Thread {

    static long starttime = System.currentTimeMillis();

    class Time {
        long time;
        long interval;

        public Time(long time, long interval) {
            this.time = time;
            this.interval = interval;
        }
    }

    private LinkedBlockingQueue<Time> data = new LinkedBlockingQueue<Time>();


    public void put(long interval) {
        Time t = new Time(System.currentTimeMillis(), interval);
        data.add(t);
    }

    public void run() {
        while (true) {
            try {
                print();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void print() throws InterruptedException {
        long t_avg = -1;
        long t_max = -1;
        long t_99 = -1;
        long t_95 = -1;
        long t_995 = -1;
        long t_999 = -1;
        long t_min = -1;
        long qps = 0;

        int p = 0;
        while (true) {
            PriorityQueue<Long> queue = new PriorityQueue<Long>();
            while (true) {
                Time time = data.poll(1, TimeUnit.SECONDS);
                if (time != null) {
                    long timems = (time.time);
                    if (starttime >= timems && starttime - 1000 < timems) {
                        queue.add(time.interval);
                    } else {
                        if (System.currentTimeMillis() - starttime <= 0) {
                            break;
                        }
                        long sleep = 1000 - (System.currentTimeMillis() - starttime);
                        if (sleep >= 0) {
                            Thread.sleep(sleep);
                        }
                        starttime = starttime + 1000l;
                        break;
                    }
                } else {
                    if (System.currentTimeMillis() - starttime <= 0) {
                        break;
                    }
                    long sleep = 1000 - (System.currentTimeMillis() - starttime);
                    if (sleep >= 0) {
                        Thread.sleep(sleep);
                    }
                    starttime = starttime + 1000l;
                    break;
                }
            }
            long sum = 0;
            long size = queue.size();
            for (int i = 0; i < size; i++) {
                long v = queue.poll();
                if (i == 0) {
                    t_min = v;
                }
                if (i == size - 1) {
                    t_max = v;
                }
                if (i == (size * 95 / 100) - 1) {
                    t_95 = v;
                }
                if (i == (size * 99 / 100) - 1) {
                    t_99 = v;
                }
                if (i == (size * 995 / 1000) - 1) {
                    t_995 = v;
                }
                if (i == (size * 999 / 1000) - 1) {
                    t_999 = v;
                }

                sum = sum + v;
            }
            if (size == 0) t_avg = 0;
            else t_avg = sum / size;
            qps = size;
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dateString = formatter.format(starttime);

            String now = formatter.format(System.currentTimeMillis());

            if (p % 10 == 0) {

                System.out.printf("|%1$20s | %2$20s|%3$8s|%4$8s|%5$8s|%6$8s|%7$8s|%8$10s|%9$10s|%10$10s|\n",
                        "now", "time", "qps", "min(ms)", "avg(ms)", "%95(ms)", "%99(ms)", "%99.5(ms)", "%99.9(ms)", "max(ms)");
            }
            p++;

            System.out.printf("| %1$20s| %2$20s|%3$8d|%4$8d|%5$8d|%6$8d|%7$8d|%8$10d|%9$10d|%10$10d|\n", now,
                    dateString, qps, t_min, t_avg, t_95, t_99, t_995, t_999, t_max);


            t_avg = -1;
            t_max = -1;
            t_99 = -1;
            t_95 = -1;
            t_995 = -1;
            t_999 = -1;
            t_min = -1;

        }
    }
}
