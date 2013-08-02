package valencia;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class HashStoreConfig {

    private final File directory;
    private int indexInitialCapacity = 1024;
    private double indexLoadFactor = 0.75d;
    private double indexExpansionFactor = 2.0;
    private int segmentSize = 128 * 1024 * 1024;
    private double minSegmentUtilization = 0.5d;
    private boolean checkCrcs = false;
    private String cleanerGroup = "default";
    private int maxCleanerBacklog = 10;
    private int maxCleanerThreads = 5;
    private long cleanerThreadKeepAliveMs = 30*1000;
    private long segmentDeletionDelayMs = 60*1000;
    private boolean singleThreaded = false;
    private Cleaner cleaner = null;
    private boolean heapAllocateIndex = false;
    private int maxRecordSize = this.segmentSize;
    private boolean saveIndexOnClose = true;
    
    public HashStoreConfig(File dir) {
        this.directory = dir;
    }

    public int indexInitialCapacity() {
        return indexInitialCapacity;
    }

    /**
     * The initial number of slots in the index. The default is 1024. You should not need to set this.
     */
    public HashStoreConfig indexInitialCapacity(int indexInitialCapacity) {
        this.indexInitialCapacity = indexInitialCapacity;
        return this;
    }

    public double indexLoadFactor() {
        return indexLoadFactor;
    }

    /**
     * The fraction of slots in the index that can be full before index expansion occurs. The default is 0.5.
     */
    public HashStoreConfig indexLoadFactor(double indexLoadFactor) {
        if(indexLoadFactor <= 0.0 || indexLoadFactor >= 1.0)
            throw new IllegalArgumentException("Invalid index load factor " + indexLoadFactor);
        this.indexLoadFactor = indexLoadFactor;
        return this;
    }

    public double indexExpansionFactor() {
        return indexExpansionFactor;
    }

    /**
     * The factor by which to expand the index when it is too full (e.g. 2.0 will double the index size). The default is 2.0.
     */
    public HashStoreConfig indexExpansionFactor(double indexExpansionFactor) {
        if(indexExpansionFactor <= 1.0)
            throw new IllegalArgumentException("Invalid index expansion factor " + indexLoadFactor);
        this.indexExpansionFactor = indexExpansionFactor;
        return this;
    }

    public int segmentSize() {
        return segmentSize;
    }

    /**
     * The size of the segment files that make up the log. The default is 128MB.
     */
    public HashStoreConfig segmentSize(int segmentSize) {
        if(segmentSize < 1 || segmentSize > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Invalid segment size " + segmentSize);
        this.segmentSize = segmentSize;
        return this;
    }

    public double minSegmentUtilization() {
        return minSegmentUtilization;
    }

    /**
     * The minimum utilization of a log segment before it will be cleaned. The default is 0.5.
     */
    public HashStoreConfig minSegmentUtilization(double minSegmentUtilization) {
        if(minSegmentUtilization <= 0.0 || minSegmentUtilization >= 1.0)
            throw new IllegalArgumentException("Invalid segment utilization "
                                               + minSegmentUtilization);
        this.minSegmentUtilization = minSegmentUtilization;
        return this;
    }

    public boolean checkCrcs() {
        return checkCrcs;
    }

    /**
     * If true this will check the CRCs of all messages as they are read. Default is false. There is some performance impact to enabling this.
     */
    public HashStoreConfig checkCrcs(boolean checkCrcs) {
        this.checkCrcs = checkCrcs;
        return this;
    }

    /**
     * The directory in which data is to be stored.
     */
    public File directory() {
        return directory;
    }
    
    public String cleanerGroup() {
        return cleanerGroup;
    }
    
    /**
     * The cleaner group for this store. All stores with the same cleaner group will share a pool of cleaner threads.
     */
    public HashStoreConfig cleanerGroup(String group) {
        this.cleanerGroup = group;
        return this;
    }

    /**
     * The maximum number of log segments by which the cleaner can fall behind before beginning to increase the number of cleaner threads.
     * If the cleaner does reach this level of backlog it will gradually increase the number of cleaner threads until it reaches the maximum
     * configured number of cleaner threads or the backlog drops.
     */
    public HashStoreConfig maxCleanerBacklog(int backlog) {
        this.maxCleanerBacklog = backlog;
        return this;
    }
    
    public int maxCleanerBacklog() {
        return this.maxCleanerBacklog;
    }
    
    /**
     * The maximum number of cleaner threads we can have
     */
    public HashStoreConfig maxCleanerThreads(int threads) {
        this.maxCleanerThreads = threads;
        return this;
    }
    
    public int maxCleanerThreads() {
        return this.maxCleanerThreads;
    }
    
    /**
     * Cleaner threads will not immediately shut themselves down when they become inactive but will instead remain alive for the
     * amount of time set by this parameter. This is to avoid churning the number of cleaner threads.
     */
    public HashStoreConfig cleanerThreadKeepAlive(long time, TimeUnit unit) {
        this.cleanerThreadKeepAliveMs = TimeUnit.MILLISECONDS.convert(time, unit);
        return this;
    }
    
    public long cleanerThreadKeepAliveMs() {
        return this.cleanerThreadKeepAliveMs;
    }
    
    /**
     * Segments are not immediately deleted from the log after being cleaned as something may still be reading from them.
     * This parameters controls a back-off time between when garbage collection has completed and when deletion can occur.
     * You should not need to set this.
     */
    public HashStoreConfig segmentDeletionDelay(long time, TimeUnit unit) {
        this.segmentDeletionDelayMs = TimeUnit.MILLISECONDS.convert(time, unit);
        return this;
    }
    
    public long segmentDeletionDelayMs() {
        return this.segmentDeletionDelayMs;
    }
    
    /**
     * Run this store in single-threaded mode (i.e. no async gc)
     */
    public HashStoreConfig singleThreaded(boolean singleThreaded) {
        this.singleThreaded = singleThreaded;
        return this;
    }
    
    public boolean singleThreaded() {
        return this.singleThreaded;
    }
    
    /**
     * Set the cleaner associated with this store. If none is set, one will be created. This option allows
     * sharing a single cleaner between multiple stores.
     */
    public HashStoreConfig cleaner(Cleaner cleaner) {
        this.cleaner = cleaner;
        return this;
    }
    
    public Cleaner cleaner() {
        return this.cleaner;
    }
    
    /**
     * Indicates whether to allocate the index on the java heap or in normal process memory.
     */
    public HashStoreConfig heapAllocateIndex(boolean heapAllocate) {
        this.heapAllocateIndex = heapAllocate;
        return this;
    }
    
    public boolean heapAllocateIndex() {
        return this.heapAllocateIndex;
    }
    
    /**
     * Set the maximum allowable record size
     */
    public HashStoreConfig maxRecordSize(int maxRecordSize) {
        if(maxRecordSize > this.segmentSize)
            throw new IllegalArgumentException("Cannot set the max record size to be larger than the segment size.");
        this.maxRecordSize = maxRecordSize;
        return this;
    }
    
    public int maxRecordSize() {
        return this.maxRecordSize;
    }
    
    /**
     * If true we will attempt to save out the contents of the index when we shut down the store
     */
    public HashStoreConfig saveIndexOnClose(boolean saveOnClose) {
        this.saveIndexOnClose = saveOnClose;
        return this;
    }
    
    public boolean saveIndexOnClose() {
        return this.saveIndexOnClose;
    }
    
}
