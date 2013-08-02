package valencia;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.google.common.io.Files;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import valencia.utils.ClosableIterator;
import valencia.utils.Utils;

public class PerformanceTest {
    
    private static final Logger logger = Logger.getLogger(PerformanceTest.class);
    
    private static final int NUM_SAMPLES = 1000;
    private static final int CHUNK_SIZE = 50;
    
    private final HashStore store;
    private final int threads;
    private final int scanThreads;
    private final int scanThreadSleepMs;
    private final long operations;
    private final int throughput;
    private final int numKeys;
    private final String keyFile;
    private final int valueSize;
    private final long reportingInterval;
    private final double reads;
    private final double writes;
    private final double deletes;
    private final long warmUp;
    private final boolean prepopulate;
    
    public PerformanceTest(HashStore store,
                           int threads,
                           int scanThreads,
                           int scanThreadSleepMs,
                           int throughput,
                           long operations,
                           int numKeys,
                           String keyFile,
                           int valueSize,
                           long reportingInterval,
                           double reads,
                           double writes,
                           double deletes,
                           long warmUp,
                           boolean prepopulate) {
        this.store = store;
        this.threads = threads;
        this.scanThreads = scanThreads;
        this.scanThreadSleepMs = scanThreadSleepMs;
        this.operations = operations;
        this.throughput = throughput;
        this.numKeys = numKeys;
        this.keyFile = keyFile;
        this.valueSize = valueSize;
        this.reportingInterval = reportingInterval;
        this.reads = reads;
        this.writes = writes;
        this.deletes = deletes;
        this.warmUp = warmUp;
        this.prepopulate = prepopulate;
        double total = this.reads + this.writes + this.deletes;
        if(Math.abs(total - 1.0) > 0.01)
            throw new IllegalArgumentException("Reads, writes and deletes should add up to 1.0, but got " + total + ".");
    }
    
    public static void main(String[] args) throws Exception {
        // parse options
        
        OptionParser parser = new OptionParser();
        OptionSpec<Integer> threadsOpt = 
                parser.accepts("threads", "The number of concurrent threads making requests.")
                      .withRequiredArg()
                      .describedAs("num_threads")
                      .ofType(Integer.class)
                      .defaultsTo(1);
        OptionSpec<Integer> scanThreadsOpt = 
                parser.accepts("scan-threads", "The number threads doing background scans on the store.")
                      .withRequiredArg()
                      .describedAs("num_threads")
                      .ofType(Integer.class)
                      .defaultsTo(0);
        OptionSpec<Integer> scanThreadSleepOpt = 
                parser.accepts("scan-thread-sleep", "The time to sleep (in ms) between scans.")
                      .withRequiredArg()
                      .describedAs("sleep_ms")
                      .ofType(Integer.class)
                      .defaultsTo(0);
        OptionSpec<Integer> throughputOpt = 
                parser.accepts("throughput", "The target throughput (req/sec) to maintain.")
                      .withRequiredArg()
                      .describedAs("req_per_sec")
                      .ofType(Integer.class)
                      .defaultsTo(-1);
        OptionSpec<Long> opsOpt = 
                parser.accepts("ops", "The total number of operations to do.")
                      .withRequiredArg()
                      .describedAs("ops")
                      .ofType(Long.class)
                      .defaultsTo(10000000L);
        OptionSpec<Long> warmupOpsOpt = 
                parser.accepts("warm-up", "The total number of put operations to do before starting the test to populate data.")
                      .withRequiredArg()
                      .describedAs("ops")
                      .ofType(Long.class)
                      .defaultsTo(0L);
        OptionSpec<String> keyFileOpt = 
                parser.accepts("key-file", "A text file with keys to use for requests, one key per line.")
                      .withRequiredArg()
                      .describedAs("file")
                      .ofType(String.class);
        OptionSpec<Integer> numKeysOpt = 
                parser.accepts("num-keys", "The number of unique keys to use if not reading keys from a file.")
                      .withRequiredArg()
                      .describedAs("count")
                      .ofType(Integer.class)
                      .defaultsTo(10000000);
        OptionSpec<Integer> valueSizeOpt = 
                parser.accepts("value-size", "The number of bytes in the values.")
                      .withRequiredArg()
                      .describedAs("bytes")
                      .ofType(Integer.class)
                      .defaultsTo(100);
        OptionSpec<Integer> reportingIntervalOpt = 
                parser.accepts("reporting-interval", "The number of seconds in between perf stat reports.")
                      .withRequiredArg()
                      .describedAs("bytes")
                      .ofType(Integer.class)
                      .defaultsTo(Integer.MAX_VALUE);
        OptionSpec<Double> percentReadsOpt = 
                parser.accepts("percent-reads", "The percentage of requests that are reads [0-100].")
                      .withRequiredArg()
                      .describedAs("percent")
                      .ofType(Double.class)
                      .defaultsTo(60.0);
        OptionSpec<Double> percentWritesOpt = 
                parser.accepts("percent-writes", "The percentage of requests that are writes [0-100].")
                      .withRequiredArg()
                      .describedAs("percent")
                      .ofType(Double.class)
                      .defaultsTo(35.0);
        OptionSpec<Double> percentDeletesOpt = 
                parser.accepts("percent-deletes", "The percentage of requests that are deletes [0-100].")
                      .withRequiredArg()
                      .describedAs("percent")
                      .ofType(Double.class)
                      .defaultsTo(5.0);
        OptionSpec<String> directoryOpt = 
                parser.accepts("directory", "The directory in which to write data.")
                      .withRequiredArg()
                      .describedAs("percent")
                      .ofType(String.class);
        OptionSpec<Integer> indexCapacityOpt = 
                parser.accepts("index-capacity", "The initial size of the index.")
                      .withRequiredArg()
                      .describedAs("size")
                      .ofType(Integer.class)
                      .defaultsTo(1024*1024);
        OptionSpec<Double> indexLoadOpt = 
                parser.accepts("index-load", "The percent full the index must be before expansion [0-100].")
                      .withRequiredArg()
                      .describedAs("percent")
                      .ofType(Double.class)
                      .defaultsTo(75.0);
        OptionSpec<Double> utilizationOpt = 
                parser.accepts("utilization", "The utilization percent at which log segments are garbage collected [0-100].")
                      .withRequiredArg()
                      .describedAs("percent")
                      .ofType(Double.class)
                      .defaultsTo(50.0);
        OptionSpec<?> singleThreadedOpt = 
                parser.accepts("single-threaded", "Run in single threaded mode.");
        OptionSpec<?> heapAllocateIndexOpt = 
                parser.accepts("heap-allocate-index", "Allocate the index on the java heap.");
        OptionSpec<?> noHeaderOpt = 
                parser.accepts("no-header", "Suppress the column name header.");
        OptionSpec<?> noTestOpt = 
                parser.accepts("no-test", "Suppress the running of the test.");
        OptionSpec<?> cleanupOpt = 
                parser.accepts("cleanup", "Delete the contents of the data directory when completed.");
        OptionSpec<?> helpOpt = 
                parser.accepts("help", "Print this help message.");
        OptionSpec<?> prepopulateOpt = 
                parser.accepts("prepopulate", "Prepopulate with sequential keys. Requires the num-keys option to be present. " + 
                                              "Unlike the warm-up option this will not generate any garbage, but is very fast.");
       
        OptionSet options = null;
        try {
            options = parser.parse(args);
        } catch(OptionException e) {
            System.err.println("ERROR: " + e.getMessage());
            parser.printHelpOn(System.err);
            System.exit(1);
        }
        
        if(options.has(helpOpt)) {
            parser.printHelpOn(System.out);
            System.exit(1);
        }
        
        int threads = options.valueOf(threadsOpt);
        int scanThreads = options.valueOf(scanThreadsOpt);
        int scanThreadSleepMs = options.valueOf(scanThreadSleepOpt);
        int throughput = options.valueOf(throughputOpt);
        long operations = options.valueOf(opsOpt);
        String keyFile = options.valueOf(keyFileOpt);
        int numKeys = options.valueOf(numKeysOpt);
        int valueSize = options.valueOf(valueSizeOpt);
        long reportingInterval = options.valueOf(reportingIntervalOpt);
        double reads = options.valueOf(percentReadsOpt) / 100.0;
        double writes = options.valueOf(percentWritesOpt) / 100.0;
        double deletes = options.valueOf(percentDeletesOpt) / 100.0;
        long warmUp = options.valueOf(warmupOpsOpt);
        int indexCapacity = options.valueOf(indexCapacityOpt);
        double utilization = options.valueOf(utilizationOpt);
        double indexLoad = options.valueOf(indexLoadOpt);
        boolean singleThreaded = options.has(singleThreadedOpt);
        boolean heapAllocate = options.has(heapAllocateIndexOpt);
        final boolean cleanup = options.has(cleanupOpt);
        boolean noHeader = options.has(noHeaderOpt);
        boolean noTest = options.has(noTestOpt);
        boolean prepopulate = options.has(prepopulateOpt);
       
        final File directory;
        if(options.has(directoryOpt))
            directory = new File(options.valueOf(directoryOpt));
        else
            directory = Files.createTempDir();
        if(!directory.exists())
            directory.mkdirs();
        
        if(options.has(keyFileOpt) && options.has(numKeysOpt)) {
            System.err.println("Cannot specify both a key file and a number of keys.");
            System.exit(1);
        }
        
        if(prepopulate && keyFile != null) {
            System.err.println("The prepopulate option is only available with the --num-keys option.");
            System.exit(1);
        }
        
        if(!noHeader)
            Metrics.printColumnNames();
        
        if(!noTest) {      
            final HashStore store = new HashStore(new HashStoreConfig(directory)
                                                  .indexInitialCapacity(indexCapacity)
                                                  .minSegmentUtilization(utilization / 100.0)
                                                  .indexLoadFactor(indexLoad / 100.0)
                                                  .singleThreaded(singleThreaded)
                                                  .heapAllocateIndex(heapAllocate));
            
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    store.close();
                    if(cleanup) {
                        File[] files = directory.listFiles();
                        if(files != null) {
                            for(File f: files)
                                Utils.delete(f);
                        }
                    }
                }
            });
            
            // log the options you got
            String opts = "";
            for(OptionSpec<?> spec: options.specs())
                opts += spec.toString() + "=" + options.valueOf(spec) + ", ";
            logger.debug("Running performance test with the following parameters: " + opts);
            
            PerformanceTest test = new PerformanceTest(store,
                                                       threads, 
                                                       scanThreads, 
                                                       scanThreadSleepMs,
                                                       throughput,
                                                       operations,
                                                       numKeys,
                                                       keyFile, 
                                                       valueSize,
                                                       1000 * reportingInterval, 
                                                       reads, 
                                                       writes, 
                                                       deletes, 
                                                       warmUp,
                                                       prepopulate);
            Metrics metrics = test.run();
            metrics.update(store);
            metrics.print();
        }
    }

    public Metrics run() {
        try {
            Metrics global = new Metrics();
            Metrics snapshot = new Metrics(global);
            
            logger.debug("Starting key generators...");
            BlockingQueue<ByteBuffer[]> keys = new ArrayBlockingQueue<ByteBuffer[]>(500);
            List<Thread> keyGenerators = new ArrayList<Thread>();
            if(this.keyFile == null) {
                keyGenerators.add(new KeyGenThread(numKeys, keys));
            } else {
                for(int i = 0; i < Math.max(threads/2, 1); i++)
                    keyGenerators.add(new KeyReaderThread(new File(keyFile), keys));
            }
            for(Thread t: keyGenerators)
                t.start();
            
            if(prepopulate) {
                logger.debug("Prepopulating the store with " + numKeys + " values.");
                writeSequentialValues(store, numKeys, valueSize);
            }
            
            if(warmUp > 0) {
                logger.debug("Doing " + warmUp + " warm up writes.");
                PerfThread warmup = new PerfThread(store, new Metrics(), warmUp, new Op[]{Op.WRITE}, false, -1, valueSize, keys);
                warmup.run();
            }
            
            logger.debug("Starting " + threads + " load generation threads.");
            List<PerfThread> perfThreads = new ArrayList<PerfThread>();
            for(int i = 0; i < threads; i++) {
                boolean throttled = throughput > 0;
                double perThreadThroughput = throughput / (double) threads;
                PerfThread thread = new PerfThread(store, snapshot, operations / threads, ops(reads, writes, deletes), throttled, perThreadThroughput, valueSize, keys);
                thread.start();
                perfThreads.add(thread);
            }
            
            logger.debug("Starting " + scanThreads + " scan threads.");
            List<ScanThread> scanThreads = new ArrayList<ScanThread>();
            for(int i = 0; i < this.scanThreads; i++) {
                ScanThread thread = new ScanThread(this.store, snapshot, scanThreadSleepMs);
                thread.start();
                scanThreads.add(thread);
            }
            
            ReporterThread reporter = new ReporterThread(store, snapshot, reportingInterval);
            reporter.start();
    
            // wait for perf threads to complete
            for(PerfThread thread: perfThreads)
                thread.join();
            
            // now interrupt the other threads and wait for them to stop
            reporter.interrupt();
            for(Thread keyGen: keyGenerators)
                keyGen.interrupt();
            for(ScanThread thread: scanThreads)
                thread.interrupt();
            for(ScanThread thread: scanThreads)
                thread.join();
            reporter.join();
            
            return global;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private void writeSequentialValues(HashStore store, int numValues, int size) throws IOException {
        ByteBuffer key = ByteBuffer.allocate(4);
        ByteBuffer value = ByteBuffer.allocate(size);
        long progress = numValues / 100;
        for(int i = 0; i < numValues; i++) {
            key.putInt(i);
            key.rewind();
            store.put(new Record(key, value));
            if(i % progress == progress - 1 && logger.isDebugEnabled()) {
                NumberFormat format = NumberFormat.getPercentInstance();
                logger.debug("Writing " + format.format(i / (double) numValues) + " complete.");
            }
        }
    }
    
    private Op[] ops(double reads, double writes, double deletes) {
        ArrayList<Op> ops = new ArrayList<Op>();
        int numReads = (int) (100 * reads);
        int numWrites = (int) (100 * writes);
        for(int i = 0; i < numReads; i++)
            ops.add(Op.READ);
        for(int i = 0; i < numWrites; i++)
            ops.add(Op.WRITE);
        while(ops.size() < 100)
            ops.add(Op.DELETE);
        Collections.shuffle(ops);
        return ops.toArray(new Op[ops.size()]);
    }
    
    private static class PerfThread extends Thread {
        
        private static AtomicInteger counter = new AtomicInteger(0);
        
        private final HashStore store;
        private final Metrics metrics;
        private final boolean throttled;
        private final double throughput;
        private final BlockingQueue<ByteBuffer[]> keys;
        private final Op[] opTypes;
        private final int valueSize;
        private long ops;
        
        public PerfThread(HashStore store, 
                          Metrics metrics, 
                          long ops, 
                          Op[] opTypes, 
                          boolean throttled,
                          double throughput, 
                          int valueSize, 
                          BlockingQueue<ByteBuffer[]> keys) {
            super("perf-thread-" + counter.getAndIncrement());
            this.metrics = metrics;
            this.store = store;
            this.ops = ops;
            this.opTypes = opTypes;
            this.throttled = throttled;
            this.throughput = throughput;
            this.keys = keys;
            this.valueSize = valueSize;
        }
        
        public void run() {
            try {
                Record record = null;
                Throttler throttler = new Throttler(this.throughput, 1000*1000);
                long ellapsed = 0;
                ByteBuffer value = ByteBuffer.allocate(this.valueSize);
                while(true) {
                    ByteBuffer[] someKeys = keys.take();
                    for(int i = 0; i < someKeys.length; i++) {
                        if(ops-- <= 0)
                            return;
                        ByteBuffer key = someKeys[i];
                        long start = System.nanoTime();
                        switch(opTypes[(int)(ops % opTypes.length)]) {                            
                            case READ:
                                record = this.store.get(key);
                                ellapsed = System.nanoTime() - start;
                                this.metrics.readLatency.record(ellapsed);
                                if(record != null)
                                    this.metrics.readBytes.record(record.size());
                                break;
                            case WRITE:
                                record = new Record(key, value);
                                this.store.put(record);
                                ellapsed = System.nanoTime() - start;
                                this.metrics.writeLatency.record(ellapsed);
                                this.metrics.writeBytes.record(record.size());
                                break;
                            case DELETE:
                                record = this.store.delete(key);
                                ellapsed = System.nanoTime() - start;
                                this.metrics.deleteLatency.record(System.nanoTime() - start);
                                break;      
                        }
                        this.metrics.hitRate.record(record == null? 0.0 : 1.0);
                        if(throttled)
                            throttler.maybeThrottle();
                    }
                }
            } catch(InterruptedException e) {
                e.printStackTrace();
                return;
            } catch(IOException e) {
                e.printStackTrace();
                return;
            }
        }
    }
    
    private static class Throttler {
        private final double nsPerOccurance;
        private final long minSleepNs;
        private long lastCallTime;
        private double sleepDeficit;
        
        public Throttler(double ratePerSec, long minSleepNs) {
            this.nsPerOccurance = (1000.0 * 1000.0 * 1000.0) / ratePerSec;
            this.minSleepNs = minSleepNs;
            this.lastCallTime = System.nanoTime(); 
        }
        
        public void maybeThrottle() throws InterruptedException {
            long now = System.nanoTime();
            long ellapsed = now - lastCallTime;
            sleepDeficit += (nsPerOccurance - ellapsed);
            if(sleepDeficit > minSleepNs) {
                int sleepMs = (int) sleepDeficit / (1000*1000);
                int sleepNs = (int) sleepDeficit % (1000*1000);
                Thread.sleep(sleepMs, sleepNs);
                this.sleepDeficit = 0;
                this.lastCallTime = System.nanoTime();
            } else {
                this.lastCallTime = now;
            }
        }
    }
    
    private static class ScanThread extends Thread {
        private static AtomicInteger counter = new AtomicInteger(0);
        
        private final HashStore store;
        private final Metrics metrics;
        private final int sleep;
        
        public ScanThread(HashStore store, Metrics metrics, int sleep) {
            super("scan-thread-" + counter.getAndIncrement());
            this.store = store;
            this.metrics = metrics;
            this.sleep = sleep;
        }
        
        public void run() {
            ClosableIterator<Record> iter = null;
            while(!isInterrupted()) {
                try {
                    iter = this.store.iterator();
                    while(iter.hasNext() && !isInterrupted()) {
                        Record record = iter.next();
                        this.metrics.scanThroughput.record(record.size());
                    }
                    iter.close();
                    sleep(sleep);
                } catch(InterruptedException e) {
                    break;
                } catch(Exception e) {
                    logger.error("Error in scan thread:", e);
                }
            }
            if(iter != null)
                iter.close();
            logger.debug("Scan thread exiting.");
        }
    }
    
    private class KeyReaderThread extends Thread {
        private final BlockingQueue<ByteBuffer[]> queue;
        private final File file;
        
        public KeyReaderThread(File file, BlockingQueue<ByteBuffer[]> queue) {
            super("key-read-thread");
            this.queue = queue;
            this.file = file;
        }
        
        public void run() {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(file));
                while(!isInterrupted()) {
                    ByteBuffer[] someKeys = new ByteBuffer[CHUNK_SIZE];
                    for(int i = 0; i < someKeys.length; i++) {
                        String line = reader.readLine();
                        ByteBuffer key = ByteBuffer.wrap(line.getBytes());
                        someKeys[i] = key;
                    }
                    queue.put(someKeys);
                }
            } catch(InterruptedException e) {
                return;
            } catch(IOException e) {
                e.printStackTrace();
                return;
            }
        }
    }
    
    private class KeyGenThread extends Thread {
        private final int numKeys;
        private final BlockingQueue<ByteBuffer[]> queue;
        
        public KeyGenThread(int numKeys, BlockingQueue<ByteBuffer[]> queue) {
            super("key-gen-thread");
            this.numKeys = numKeys;
            this.queue = queue;
        }
        
        public void run() {
            try {
                Random random = new Random();
                while(!isInterrupted()) {
                    ByteBuffer[] someKeys = new ByteBuffer[CHUNK_SIZE];
                    for(int i = 0; i < someKeys.length; i++) {
                        ByteBuffer key = ByteBuffer.allocate(4);
                        key.putInt(random.nextInt(numKeys));
                        key.rewind();
                        someKeys[i] = key;
                    }
                    queue.put(someKeys);
                }
            } catch(InterruptedException e) {
                return;
            }
        }
    }
    
    private class ReporterThread extends Thread {
     
        private final Metrics metrics;
        private final HashStore store;
        private final long sleepMs;
        
        public ReporterThread(HashStore store, Metrics metrics, long sleepMs) {
            super("reporter-thread");
            this.metrics = metrics;
            this.store = store;
            this.sleepMs = sleepMs;
            this.setDaemon(true);
        }

        public void run() {
            while(!isInterrupted()) {
                try {
                    sleep(sleepMs);
                    metrics.update(store);
                    metrics.print();
                    metrics.reset();
                } catch(InterruptedException e) {
                    return;
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    private static enum Op {READ, WRITE, DELETE};
    
    private static class Metrics {
        private static int WIDTH = 13;
        public Metric readLatency;
        public Metric readBytes;
        public Metric writeLatency;
        public Metric writeBytes;
        public Metric deleteLatency;
        public Metric allLatency;
        public Metric hitRate;
        public Metric scanThroughput;
        public Metric logSize;
        public Metric logUtilization;
        public Metric gcBacklog;
        public Metric gcProcessed;
        public Metric gcCleaned;
        public Metric indexSize;
        public Metric indexCapacity;
        public Metric indexCollisions;
        
        private long lastCleanerProcessed = 0;
        private long lastCleanerCleaned = 0;
        private long lastCollisions = 0;
        
        public Metrics(Metrics parent) {
            this.allLatency = new Metric("all-latency", NUM_SAMPLES);
            this.hitRate = new Metric("hit-rate", NUM_SAMPLES, parent.hitRate);
            this.readLatency = new Metric("read-latency", NUM_SAMPLES, parent.readLatency, allLatency);
            this.readBytes = new Metric("bytes-read", NUM_SAMPLES, parent.readBytes);
            this.writeLatency = new Metric("write-latency", NUM_SAMPLES, parent.writeLatency, allLatency);
            this.writeBytes = new Metric("write-read", NUM_SAMPLES, parent.writeBytes);
            this.deleteLatency = new Metric("delete-latency", NUM_SAMPLES, parent.deleteLatency);
            this.scanThroughput = new Metric("scan-throughput", NUM_SAMPLES, parent.scanThroughput);
            this.logSize = new Metric("log-size", NUM_SAMPLES, parent.logSize);
            this.logUtilization = new Metric("log-utilization", NUM_SAMPLES, parent.logUtilization);
            this.gcBacklog = new Metric("gc-backlog", NUM_SAMPLES, parent.gcBacklog);
            this.gcProcessed = new Metric("gc-processed", NUM_SAMPLES, parent.gcProcessed);
            this.gcCleaned = new Metric("gc-cleaned", NUM_SAMPLES, parent.gcCleaned);
            this.indexSize = new Metric("index-size", NUM_SAMPLES, parent.indexSize);
            this.indexCapacity = new Metric("index-capacity", NUM_SAMPLES, parent.indexCapacity);
            this.indexCollisions = new Metric("index-collisions", NUM_SAMPLES, parent.indexCollisions);
        }
        
        public Metrics() {
            this.allLatency = new Metric("all-latency", NUM_SAMPLES);
            this.hitRate = new Metric("hit-rate", NUM_SAMPLES);
            this.readLatency = new Metric("read-latency", NUM_SAMPLES, allLatency);
            this.readBytes = new Metric("bytes-read", NUM_SAMPLES);
            this.writeLatency = new Metric("write-latency", NUM_SAMPLES, allLatency);
            this.writeBytes = new Metric("write-read", NUM_SAMPLES);
            this.deleteLatency = new Metric("delete-latency", NUM_SAMPLES);
            this.scanThroughput = new Metric("scan-throughput", NUM_SAMPLES);
            this.logSize = new Metric("log-size", NUM_SAMPLES);
            this.logUtilization = new Metric("log-size", NUM_SAMPLES);
            this.gcBacklog = new Metric("gc-backlog", NUM_SAMPLES);
            this.gcProcessed = new Metric("gc-processed", NUM_SAMPLES);
            this.gcCleaned = new Metric("gc-cleaned", NUM_SAMPLES);
            this.indexSize = new Metric("index-size", NUM_SAMPLES);
            this.indexCapacity = new Metric("index-capacity", NUM_SAMPLES);
            this.indexCollisions = new Metric("index-collisions", NUM_SAMPLES);
        }
        
        public synchronized void update(HashStore store) {
            // update log stats
            this.logSize.record(store.dataSize());
            this.logUtilization.record(store.utilization());
            
            // update index stats
            this.indexSize.record(store.count());
            this.indexCapacity.record(store.indexCapacity());
            lastCollisions = store.collisions() - lastCollisions;
            long reqs = this.allLatency.count();
            this.indexCollisions.record(lastCollisions / (double) reqs);
            
            // update cleaner stats
            Cleaner cleaner = store.cleaner();
            long processed = cleaner.bytesProcessed() - lastCleanerProcessed;
            lastCleanerProcessed = cleaner.bytesProcessed();
            long cleaned = cleaner.bytesCleaned() - lastCleanerCleaned;
            lastCleanerCleaned = cleaner.bytesCleaned();
            this.gcBacklog.record(cleaner.backlog());
            this.gcProcessed.record(processed);
            this.gcCleaned.record(cleaned);                          
        }
        
        public synchronized void reset() {
            this.allLatency.reset();
            this.hitRate.reset();
            this.readLatency.reset();
            this.readBytes.reset();
            this.writeLatency.reset();
            this.writeBytes.reset();
            this.deleteLatency.reset();
            this.scanThroughput.reset();
            this.logSize.reset();
            this.logUtilization.reset();
            this.gcBacklog.reset();
            this.gcProcessed.reset();
            this.gcCleaned.reset();
            this.indexSize.reset();
            this.indexCapacity.reset();
            this.indexCollisions.reset();
        }
        
        public static void printColumnNames() {
            String format = "%" + WIDTH + "s";
            printRequestCols("all_");
            System.out.printf(format, "hit_rate");
            printRequestCols("read_");
            System.out.printf(format, "read_rate");
            printRequestCols("write_");
            System.out.printf(format, "write_rate");
            printRequestCols("del_");
            System.out.printf(format, "scan_rate");
            System.out.printf(format, "log_size");    
            System.out.printf(format, "log_utilized");
            System.out.printf(format, "gc_backlog");
            System.out.printf(format, "gc_processed");
            System.out.printf(format, "gc_cleaned");
            System.out.printf(format, "gc_rate");
            System.out.printf(format, "records");
            System.out.printf(format, "idx_size");
            System.out.printf(format, "idx_colsn");
            System.out.println();
        }
        
        private static void printRequestCols(String prefix) {
            String[] cols = {"reqs", "qps", "avg", "50th", "95th", "99th", "99_9th"};
            for(String col: cols)
                System.out.print(String.format("%" + WIDTH +"s", prefix + col));
        }
        
        public synchronized void print() {
            String decimalFormat = "%" + WIDTH + "d";
            printRequest(this.allLatency);
            System.out.printf(floatFmt(2), this.hitRate.avg());
            printRequest(this.readLatency);
            System.out.printf(floatFmt(0), this.readBytes.rate());
            printRequest(this.writeLatency);
            System.out.printf(floatFmt(0), this.writeBytes.rate());
            printRequest(this.deleteLatency);
            System.out.printf(floatFmt(0), this.scanThroughput.rate());
            System.out.printf(decimalFormat, (long) this.logSize.max());
            System.out.printf(floatFmt(3), this.logUtilization.avg());
            System.out.printf(floatFmt(1), this.gcBacklog.avg());
            System.out.printf(floatFmt(0), this.gcProcessed.avg());
            System.out.printf(floatFmt(0), this.gcCleaned.avg());
            System.out.printf(floatFmt(0), this.gcProcessed.rate());
            System.out.printf(floatFmt(0), this.indexSize.max());
            System.out.printf(floatFmt(0), this.indexCapacity.max());
            System.out.printf(floatFmt(4), this.indexCollisions.max());
            System.out.println();
        }
        
        private void printRequest(Metric m) {
            System.out.printf("%" + WIDTH + "d", m.count());
            System.out.printf(floatFmt(1), m.occuranceRate());
            System.out.printf(floatFmt(5), m.avg() / (1000.0*1000.0));
            double[] quantiles = m.quantiles(0.5, 0.95, 0.99, 0.999);
            for(double q: quantiles)
                System.out.printf(floatFmt(5), q / (1000.0*1000.0));
        }
        
        private String floatFmt(int decimals) {
            return "%" + WIDTH + "." + decimals + "f";
        }
    }
    
    private static class Metric {
        private final String name;
        private final int numSamples;
        private long start;
        private long count;
        private double total;
        private double max;
        private Sampler sampler;
        private Metric[] parents;

        public Metric(String name, int samples) {
            this(name, samples, new Metric[0]);
        }
        
        public Metric(String name, int samples, Metric... parents) {
            this.name = name;
            this.numSamples = samples;
            this.count = 0L;
            this.total = 0.0d;
            this.max = Double.MIN_VALUE;
            this.sampler = new Sampler(samples, System.nanoTime());
            this.start = System.nanoTime();
            this.parents = parents;
        }

        public synchronized void record(double v) {
            this.max = Math.max(v, this.max);
            this.total += v;
            this.count++;
            this.sampler.sample(v);
            for(Metric parent: parents)
                parent.record(v);
        }
        
        private double ellapsedSecs() {
            long now = System.nanoTime();
            return (now - start) / (1000.0 * 1000.0 * 1000.0);
        }
        
        public String name() {
            return this.name;
        }
        
        public synchronized double rate() {
            return this.total / ellapsedSecs();
        }
        
        public synchronized double max() {
            return this.max;
        }
        
        public synchronized double occuranceRate() {
            return this.count / ellapsedSecs();
        }

        public synchronized double avg() {
            return total / count;
        }
        
        public synchronized long count() {
            return this.count;
        }
        
        public synchronized double[] quantiles(double...qs) {
            return this.sampler.quantiles(qs);
        }
        
        public synchronized void reset() {
            this.count = 0L;
            this.total = 0.0d;
            this.max = Double.MIN_VALUE;
            this.start = System.nanoTime();
            this.sampler = new Sampler(this.numSamples, System.nanoTime());
        }
    }
    
    /**
     * Resevoir sampler
     */
    public static class Sampler {
        private final Random rand;
        private final double[] values;
        private long count;
        
        public Sampler(int size, long seed) {
            this.values = new double[size];
            this.count = 1;
            this.rand = new Random(seed);
        }
        
        public void sample(double v) {
            if(this.count <= this.values.length) {
                this.values[(int) count - 1] = v;
                this.count++;
            } else {
                this.count++;
                long r = Math.abs((long) rand.nextLong() % this.count);
                if(r < this.values.length)
                    this.values[(int) r] = v;
            }          
        }
        
        public double[] quantiles(double... qs) {
            int size = Math.min(this.values.length, (int) count);
            double[] copy = new double[size];
            System.arraycopy(this.values, 0, copy, 0, size);
            Arrays.sort(this.values);
            double[] results = new double[qs.length];
            for(int i = 0; i < qs.length; i++)
                results[i] = this.values[(int) Math.round(qs[i] * (size-1))];
            return results;
        }
        
        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append('[');
            for(int i = 0; i < this.values.length; i++) {
                b.append(this.values[i]);
                b.append(' ');
            }
            b.append(']');
            return b.toString();
        }
        
        public static void main(String[] args) {
            System.out.println("Random samples of {0...99}");
            for(int i = 0; i < 10; i++) {
                Sampler s = new Sampler(10, i);
                for(int j = 0; j < 100; j++)
                    s.sample(j);
                System.out.println(s);
            }
        }
    }
    
}
