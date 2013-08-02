package valencia;

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Random;
import java.io.File;
import java.nio.*;

import valencia.utils.Utils;

import com.google.common.io.*;

public class HashStoreSmack {

    static Metric deleteMetric = new Metric("delete", 20000);
    static Metric deleteExistingMetric = new Metric("delete-existing", 20000);
    static Metric putMetric = new Metric("put", 20000);
    static Metric putExistingMetric = new Metric("put-existing", 20000);
    static Metric getMetric = new Metric("get", 20000);
    static Metric getExistingMetric = new Metric("get-existing", 20000);

    static ByteBuffer keyBuffer = null;
    static ByteBuffer valBuffer = null;

    public static void main(String[] args) throws Exception {
        if(args.length < 4) {
            System.err.println("USAGE: cmd keys ops value_size reporting_interval");
            System.exit(1);
        }
        int numKeys = Integer.parseInt(args[0]);
        int numOps = Integer.parseInt(args[1]);
        int valueSize = Integer.parseInt(args[2]);
        int reportingInterval = Integer.parseInt(args[3]);
        int[] rightAnswer = new int[numKeys];

        Random random = new Random(1);
        keyBuffer = ByteBuffer.allocate(4);
        valBuffer = ByteBuffer.allocate(valueSize);
        random.nextBytes(valBuffer.array());

        File dir = Files.createTempDir();
        System.out.println("Creating data in " + dir.getAbsolutePath());
        HashStore store = new HashStore(new HashStoreConfig(dir));

        for(int iter = 0; iter < numOps; iter++) {
            int key = random.nextInt(numKeys);
            int opCode = iter % 10;
            boolean existing = rightAnswer[key] > 0;

            // set up key and value
            writeTo(key, keyBuffer);
            writeTo(rightAnswer[key] + 1, valBuffer);

            if(opCode == 0) {
                rightAnswer[key] = 0;
                delete(store, existing);
            } else if(opCode < 5) {
                rightAnswer[key] += 1;
                put(store, existing);
            } else {
                get(store, existing);
            }
            if(iter % reportingInterval == reportingInterval - 1) {
                NumberFormat format = NumberFormat.getInstance();
                System.out.println("Iteration " + format.format(iter) + "/" + format.format(numOps) + ", size = " + format.format(store.count()));
                System.out.println(String.format("%20s %15s %15s %15s %15s %15s %15s",
                                                 "Operation",
                                                 "Req/sec",
                                                 "Avg (ms)",
                                                 "95th",
                                                 "99th",
                                                 "99.9th",
                                                 "Max (ms)"));
                for(Metric metric: Arrays.asList(getMetric,
                                                 getExistingMetric,
                                                 putMetric,
                                                 putExistingMetric,
                                                 deleteMetric,
                                                 deleteExistingMetric))
                    System.out.println(String.format("%20s %15.5f %15.5f %15.5f %15.5f %15.5f %15.5f",
                                                     metric.name,
                                                     (1000.0 * 1000.0 * 1000.0) / metric.avg(),
                                                     metric.avg() / (1000.0 * 1000.0),
                                                     metric.quantile(0.95) / (1000.0 * 1000.0),
                                                     metric.quantile(0.99) / (1000.0 * 1000.0),
                                                     metric.quantile(0.999) / (1000.0 * 1000.0),
                                                     metric.maximum / (1000.0 * 1000.0)));
            }
        }

        System.out.println("Verifying correctness...");
        for(int i = 0; i < numKeys; i++) {
            writeTo(i, keyBuffer);
            Record found = store.get(keyBuffer);
            if(found == null) {
                if(rightAnswer[i] != 0)
                    System.out.println("Expected 0 but found null for key " + i);
            } else {
                int foundVal = found.value().getInt();
                if(foundVal != rightAnswer[i])
                    System.out.println("Expected " + rightAnswer[i] + " but found "
                                       + found.value().getInt() + " for key " + i);

            }
        }
        System.out.println("Deleting store.");
        Utils.delete(dir);
    }

    static void writeTo(int value, ByteBuffer buffer) {
        buffer.position(0);
        buffer.putInt(value);
        buffer.rewind();
    }

    static void delete(HashStore store, boolean existing) throws Exception {
        long start = System.nanoTime();
        store.delete(keyBuffer);
        long ellapsed = System.nanoTime() - start;
        if(existing)
            deleteExistingMetric.record(ellapsed);
        else
            deleteMetric.record(ellapsed);
    }

    static void get(HashStore store, boolean existing) throws Exception {
        long start = System.nanoTime();
        store.get(keyBuffer);
        long ellapsed = System.nanoTime() - start;
        if(existing)
            getExistingMetric.record(ellapsed);
        else
            getMetric.record(ellapsed);
    }

    static void put(HashStore store, boolean existing) throws Exception {
        Record record = new Record(keyBuffer.array(), valBuffer.array());
        long start = System.nanoTime();
        store.put(record);
        long ellapsed = System.nanoTime() - start;
        if(existing)
            putExistingMetric.record(ellapsed);
        else
            putMetric.record(ellapsed);

    }

    private static class Metric {
        String name;
        long count = 0L;
        double metric = 0.0d;
        double maximum = Double.MIN_VALUE;
        double[] values;
        int valueIdx = 0;

        public Metric(String name, int samples) {
            this.name = name;
            this.values = new double[samples];
        }

        public void record(double v) {
            this.maximum = Math.max(v, this.maximum);
            this.metric += v;
            this.count++;
            this.valueIdx = (this.valueIdx + 1) % this.values.length;
            this.values[valueIdx] = v;
        }

        public double avg() {
            return metric / count;
        }
        
        public double quantile(double q) {
            int size = Math.min(this.values.length, (int) count);
            Arrays.sort(this.values, 0, size);
            return this.values[(int) Math.round(q * size)];
        }
    }

}