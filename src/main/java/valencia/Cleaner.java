package valencia;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import valencia.utils.ClosableIterator;
import valencia.utils.DelayedItem;


/**
 * Cleans the log by re-writing active records.
 * 
 * The cleaner can be shared across many stores, or each store can have its own cleaner. The cleaner has a pool of 
 * threads that will do the cleaning work. This pool will expand if needed.
 */
public class Cleaner {

    private static final Logger logger = Logger.getLogger(Cleaner.class);
        
    private final String name;
    private final AtomicBoolean running;
    private final int maxBacklog;
    private final int maxThreads;
    private final long deleteDelay;
    private final BlockingQueue<DirtySegment> garbage;
    private final DelayQueue<DirtySegment> deletions;
    private final Set<CleanerThread> threads;
    private final AtomicInteger references;
    private final AtomicLong bytesCleaned;
    private final AtomicLong bytesProcessed;
    private final boolean autoShutdown;
    private final boolean isAsync;
    
    /**
     * Create a cleaner
     * @param name The name of this cleaner pool
     * @param maxThreads The maximum number of cleaning threads that can be created
     * @param maxBacklog The maximum backlog of under-utilized segments before we create more cleaner threads.
     * @param childMinTimeToLiveMs The amount of time to keep idle clenaer threads alive
     * @param deleteDelay The amount of time to wait before deleting an empty segment
     * @param autoShutdown Should we automatically shutdown this cleaner when no more references remain?
     */
    public Cleaner(String name, 
                   int maxThreads, 
                   int maxBacklog, 
                   long childMinTimeToLiveMs, 
                   long deleteDelay, 
                   boolean autoShutdown, 
                   boolean isAsync) {
        this.name = name;
        this.maxThreads = maxThreads;
        this.maxBacklog = maxBacklog;
        this.deleteDelay = deleteDelay;
        this.running = new AtomicBoolean(true);
        this.garbage = new LinkedBlockingQueue<DirtySegment>();
        this.deletions = new DelayQueue<DirtySegment>();
        this.threads = Collections.synchronizedSet(new HashSet<CleanerThread>());
        this.references = new AtomicInteger(0);
        this.bytesCleaned = new AtomicLong(0);
        this.bytesProcessed = new AtomicLong(0);
        this.autoShutdown = autoShutdown;
        this.isAsync = isAsync;
        
        // start the root cleaner thread if in async mode
        if(this.isAsync) {
            CleanerThread adam = new CleanerThread(this, true, childMinTimeToLiveMs);
            adam.start();
            this.threads.add(adam);
        }
    }
    
    /**
     * Clean the given segment
     */
    void clean(HashStore store, LogSegment segment) throws IOException {
        if(isAsync) {
            if(logger.isTraceEnabled())
                logger.trace("Segment " + segment.baseOffset() + " queued up for cleaning.");
            try {
                this.garbage.put(new DirtySegment(store, segment, 0));
            } catch(InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            // gc the segment synchonously
            gc(store, segment);                       
            store.log().delete(segment, false);
            segment.delete();
        }
    }
    
    /**
     * The name of this cleaner
     */
    public String name() {
        return this.name;
    }
    
    /**
     * Shutdown this cleaner
     */
    public void shutdown() {
        this.running.set(false);
        try {
            for(CleanerThread thread: this.threads)
                thread.join();
        } catch(InterruptedException e) {
            logger.error("Failed to shutdown cleaner thread.", e);
        }
    }
    
    /**
     * The maximum number of threads this cleaner can create
     */
    public int maxThreads() {
        return this.maxThreads;
    }
    
    public int backlog() {
        return this.garbage.size();
    }
    
    public int maxBacklog() {
        return this.maxBacklog;
    }
    
    public long bytesProcessed() {
        return this.bytesProcessed.get();
    }
    
    public long bytesCleaned() {
        return this.bytesCleaned.get();
    }
    
    int runningThreads() {
        return this.threads.size();
    }
    
    boolean running() {
        return running.get();
    }
    
    void reference() {
        this.references.getAndIncrement();
    }
    
    void dereference() {
        int references = this.references.getAndDecrement();
        if(references < 0)
            throw new IllegalStateException("This cleaner has been dereferenced more times then it has been refererenced. This should not be possible.");
        if(references == 0 && this.autoShutdown)
            this.shutdown();           
    }
    
    private void delayedDelete(DirtySegment segment) {
        this.deletions.put(new DirtySegment(segment.store(), segment.segment(), deleteDelay));
    }
    
    private void recordNewThread(CleanerThread thread) {
        this.threads.add(thread);
    }
    
    private void removeThread(CleanerThread thread) {
        this.threads.remove(thread);
    }
    
    private DirtySegment pollDeletable() {
        while(true) {
            DirtySegment seg = this.deletions.poll();
            if(seg == null)
                return null;
            // if it still has live references we can't delete it yet, re-enqueue it
            if(seg.segment().hasReferences()) {
                int attempts = seg.recordAttempt();
                if(attempts > 10 && logger.isDebugEnabled())
                    logger.debug("Could not delete segment after " + attempts + " is this a leak?");
                seg.resetDelay();
                this.deletions.add(seg);  
            } else {
                return seg;
            }
        }
    }
    
    private DirtySegment pollGarbage(long ms) throws InterruptedException {
        return this.garbage.poll(ms, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Recopy any active records in this segment to the front of the log and
     * then delete this segment
     */
    private void gc(HashStore store, LogSegment segment) throws IOException {
        long start = System.nanoTime();
        long startSize = segment.size();
        int bytesCopied = 0;
        ClosableIterator<LogEntry> iter = segment.iterator();
        try {
            while(iter.hasNext()) {
                LogEntry entry = iter.next();
                boolean recopied = store.rewriteRecordIfActive(entry.record(), entry.offset());
                int bytes = 4 + entry.record().size();
                if(recopied)
                    bytesCopied += bytes;
            }
        } finally {
            iter.close();
        }
        long freed = startSize - bytesCopied;
        this.bytesProcessed.addAndGet(startSize);
        this.bytesCleaned.addAndGet(freed);
        if(logger.isDebugEnabled()) {
            double ellapsed = (System.nanoTime() - start) / (1000.0 * 1000.0);
            logger.debug(String.format("Garbage collected segment %s, freeing %.3f MB of %.3f MB in %.2f ms. Backlog is " + backlog() + " segments.",
                                       segment.file().getName(),
                                       freed / (1024.0 * 1024.0),
                                       startSize / (1024.0*1024.0),
                                       ellapsed));
        }
    }
    
    private static class DirtySegment extends DelayedItem {
        private final LogSegment segment;
        private final HashStore store;
        private final AtomicInteger attempts;
        
        public DirtySegment(HashStore store, LogSegment segment, long delay) {
            super(delay);
            this.store = store;
            this.segment = segment;
            this.attempts = new AtomicInteger(0);
        }
        
        public LogSegment segment() {
            return this.segment;
        }
        
        public HashStore store() {
            return this.store;
        }
        
        public int attempts() {
            return this.attempts.get();
        }
        
        public int recordAttempt() {
            return this.attempts.getAndIncrement();
        }
        
    }
    
    /**
     * A background thread that does garbage collection. This thread will spawn other threads if it falls behind
     * on cleaning, but these child threads will not be able to themselves spawn any threads.
     */
    private static class CleanerThread extends Thread {
        
        private static AtomicInteger cleanerId = new AtomicInteger(0);
        
        private final Cleaner cleaner;
        private boolean running;
        private final boolean isRootThread;
        private final long childMinTimeToLiveMs;
        
        public CleanerThread(Cleaner cleaner, boolean isRootThread, long childMinTimeToLiveMs) {
            super("valencia-cleaner-" + cleaner.name() + "-" + cleanerId.getAndIncrement());
            this.setDaemon(true);
            this.cleaner = cleaner;
            this.running = true;
            this.isRootThread = isRootThread;
            this.childMinTimeToLiveMs = childMinTimeToLiveMs;
        }
        
        public void run() {  
            logger.debug("Starting cleaner thread " + getName());
            while(cleaner.running() && running) {
                try {                                     
                    // add more cleaner threads if needed
                    if(shouldSpawn())
                        spawn();
                    
                    // delete any cleaned segments
                    processDeletions();
                    
                    // gc a segment
                    DirtySegment segment = cleaner.pollGarbage(childMinTimeToLiveMs);
                    if(segment == null) {
                        // we got nothing, there isn't enough work for us to do, shut down this auxilery thread
                        if(!isRootThread)
                            killSelf();
                    } else {
                        // gc the segment
                        cleaner.gc(segment.store(), segment.segment());                       
                        cleaner.delayedDelete(segment);
                    }
                } catch(Exception e) {
                    logger.error("Error in cleaner thread:", e);
                }
            }
            logger.debug("Cleaner thread " + getName() + " shutting down.");
        }
        
        /**
         * Delete any segments in the delete queue that are ready for processing
         */
        private void processDeletions() {
            while(true){
                DirtySegment d = cleaner.pollDeletable();
                if(d == null)
                    break;
                d.store().log().delete(d.segment(), false);
                d.segment().delete();
            }
        }
        
        /**
         * Stop the current thread's event loop and remove it from the active thread list
         */
        private void killSelf() {
            logger.info("Shutting down auxilery cleaner thread " + getName() + " due to inactivity.");
            cleaner.removeThread(this);
            this.running = false;
        }
        
        /**
         * A thread should create more cleaner threads if it is the root thread, it has fewer than the maximum number of running threads
         * and the backlog is greater than the max backlog.
         * @return
         */
        private boolean shouldSpawn() {
            return isRootThread && cleaner.runningThreads() < cleaner.maxThreads() && cleaner.backlog() > cleaner.maxBacklog();
        }
        
        /**
         * Create a new cleaner thread if (1) the backlog is high enough to warrant it and (2) we are the root thread, and (3)
         * we are not already at the maximum number of cleaner threads
         */
        private void spawn() {
            logger.info("Spawning cleaner thread number " + (cleaner.runningThreads() + 1) + ", backlog of " + cleaner.backlog() + " segments.");
            CleanerThread thread = new CleanerThread(cleaner, false, childMinTimeToLiveMs);
            thread.start();
            cleaner.recordNewThread(thread);
        }
    }
}
