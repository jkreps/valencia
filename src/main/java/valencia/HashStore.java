package valencia;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.apache.log4j.Logger;

import valencia.utils.ClosableIterator;
import valencia.utils.Crc32;
import valencia.utils.FakeLock;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * A hash indexed log-structured embedded key-value store.
 *
 */
public class HashStore {

    private static Logger logger = Logger.getLogger(HashStore.class);

    private final HashStoreConfig config;
    private final Index index;
    private final Log log;
    private final Cleaner cleaner;
    private final AtomicBoolean open;

    public HashStore(HashStoreConfig config) throws IOException {
        this.config = config;
        this.index = new Index(config.singleThreaded(), config.heapAllocateIndex());
        config.directory().mkdirs();
        this.log = new Log(config.directory(), config.maxRecordSize(), config.segmentSize());
        this.open = new AtomicBoolean(true);
        recover();
        
        // now that the log is recovered, start the cleaner
        if(config.cleaner() == null)
            this.cleaner = new Cleaner(config.cleanerGroup(), 
                                       config.maxCleanerThreads(), 
                                       config.maxCleanerBacklog(), 
                                       config.cleanerThreadKeepAliveMs(), 
                                       config.segmentDeletionDelayMs(), 
                                       true,
                                       !config.singleThreaded());
        else
            this.cleaner = config.cleaner();
        this.cleaner.reference();
    }

    /**
     * Iterate over the log and rebuild the index
     * 
     * @throws IOException
     */
    private void recover() throws IOException {
        long start = System.nanoTime();
        File savedIndex = new File(config.directory(), "index");
        if(config.saveIndexOnClose() && savedIndex.exists()) {
            try {
                logger.debug("Attempting to recover index from saved file.");
                index.readFrom(savedIndex);
            } catch(IOException e) {
                logger.warn("Failed to read saved index file: ", e);
                recoverByFullLogScan();
            }
        } else {
            recoverByFullLogScan();
        }
        double ellapsed = (System.nanoTime() - start) / (1000.0 * 1000.0 * 1000.0);
        logger.debug(String.format("Recovered %s bytes in %.4f seconds.",
                                  log.length(),
                                  ellapsed));
    }
    
    private void recoverByFullLogScan() throws IOException {
        logger.info("Recovering index by full log scan.");
        long lastValidOffset = -1;
        ClosableIterator<LogEntry> iter = null;
        try {
            iter = log.iterator(true);
            while(iter.hasNext()) {
                LogEntry entry = iter.next();
                putInIndex(entry.record().key(), entry.offset());
                lastValidOffset = entry.offset() + entry.record().size() + 4;
            }
        } catch(LogCorruptionException e) {
            logger.info("Tail of the log is invalid (" + e.getMessage() + "), truncating...");
            log.truncateTo(lastValidOffset);
        } finally {
            if(iter != null)
                iter.close();
        }
    }

    /**
     * Return the record if the key is found or null otherwise.
     */
    public Record get(ByteBuffer key) throws IOException {
        IndexSlotSnapshot entry = index.get(key);
        if(logger.isTraceEnabled())
            logger.trace("find() = (slot = " + entry.slot() + ", hash = " + entry.hash() + ", offset = " + entry.offset()
                         + ", record = " + entry.record() + ")");
        return entry.record();
    }

    /**
     * Store this record.
     * @return The previous record stored for this key (or null if there is
     *         none)
     */
    public Record put(Record record) throws IOException {
        long offset = log.append(record);
        return putInIndex(record.key(), offset);
    }

    /**
     * Enter this key=>offset pair into the index, and update the log segment stats to reflect the change in utilization.
     */
    private Record putInIndex(ByteBuffer key, long offset) throws IOException {
        IndexSlotSnapshot entry = index.put(key, offset);
        // if this is an update, mark the old record deleted  
        if(entry.empty()) {
            return null;
        } else {
            LogSegment segment = log.segmentFor(entry.offset());
            if(segment != null) {
                segment.recordRecordDeletion();
                maybeClean(segment);
            }
            return entry.record();
        }
    }
    
    /**
     * Any segment not being currently gc'd that is read-only and is under-utilized is available for collection.
     * 
     * This method will check this, and attempt to set the segment invalid
     */
    private void maybeClean(LogSegment segment) throws IOException {
        boolean canClean = segment.valid() && !segment.mutable() && segment.utilization() < config.minSegmentUtilization();
        if(canClean) {
            // check it isn't already invalidated by another thread so we don't double-submit it for cleaning
            boolean iAmCleaner = segment.invalidate();
            if(iAmCleaner)
                this.cleaner.clean(this, segment);
        }
    }

    /**
     * Delete the given key from the index if it exists
     * 
     * @param key The key to delete
     * @return The record previously stored, if one is deleted, otherwise null
     */
    public Record delete(ByteBuffer key) throws IOException {
        IndexSlotSnapshot entry = index.delete(key);
        if(entry.empty())
            return null;
        log.markDeleted(entry.offset());
        return entry.record();
    }

    /**
     * @return The configuration for this store
     */
    public HashStoreConfig config() {
        return this.config;
    }
    
    /**
     * Get the cleaner associated with this store.
     */
    public Cleaner cleaner() {
        return this.cleaner;
    }

    /** 
     * Close this store
     */
    public void close() {
        this.cleaner.dereference();
        this.log.close();
        if(config.saveIndexOnClose()) {
            try {
                this.index.saveTo(new File(this.config.directory(), "index"));
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** 
     * The number of active records in the store
     */
    public int count() {
        ensureOpen();
        return this.index.size();
    }
    
    /**
     * The percentage of records in the log that are actively referenced from the index
     */
    public double utilization() {
        ensureOpen();
        return this.log.utilization();
    }
    
    /**
     * Total amount of memory allocated for the index in bytes
     */
    public long indexSize() {
        ensureOpen();
        return indexCapacity() * ((long) Integer.SIZE + Long.SIZE) / 8L;
    }
    
    /**
     * The current maximum number of entries of the index
     */
    public long indexCapacity() {
        ensureOpen();
        return index.capacity();
    }
    
    /**
     * Total amount of data on disk, including active and inactive records
     */
    public long dataSize() {
        ensureOpen();
        return log.length();
    }
    
    /**
     * Return true if this store is open.
     */
    public boolean isOpen() {
        return this.open.get();
    }
    
    /**
     * The number of hash collisions on this index
     */
    public long collisions() {
        return this.index.collisions();
    }
    
    /**
     * Recopy the record to the end of the log and update the index
     */
     boolean rewriteRecordIfActive(Record record, long offset) throws IOException {
        int slot = index.slotHolding(record.key(), offset);
        // if this is a valid record, recopy it, else ignore it
        if(slot >= 0) {
            long newOffset = log.append(record);
            boolean success = index.testAndSetOffset(slot, offset, newOffset);
            // if the recopy wasn't clean, mark the new record deleted and try again
            if(!success)
                log.markDeleted(newOffset);
            return success;
        }
        return false;
    }
     
    /**
     * Get the log for this store.
     */
    Log log() {
        return this.log;
    }
    
    public ClosableIterator<Record> iterator() {
        return new ClosableIterator<Record>() {
            private final ClosableIterator<LogEntry> iter = log.iterator();
            private Record current = null;
            
            public boolean hasNext() {
                return makeNext();
            }

            public Record next() {
                if(makeNext()) {
                    Record record = this.current;
                    this.current = null;
                    return record;
                } else {
                    throw new NoSuchElementException();
                }
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
            
            private boolean makeNext() {
                // if we already have a record ready, use that
                if(this.current != null)
                    return true;
                
                // otherwise try to find a new, valid record
                while(iter.hasNext()) {
                    LogEntry entry = iter.next();
                    int slot = index.slotHolding(entry.record().key(), entry.offset());
                    if(slot >= 0) {
                        this.current = entry.record();
                        return true;
                    }
                }
                return false;
            }
            
            public void close() {
                iter.close();
            }
        };
    }
    
    private void ensureOpen() {
        if(!open.get())
            throw new IllegalStateException("This store has been closed.");
    }
    
    private static interface IndexArray {
        public long offset(int slot);
        public void offset(int slot, long offset);
        public int hash(int slot);
        public void hash(int slot, int hash);
        public void set(int slot, int hash, long offset);
        public int capacity();
        public void close();
    }
    
    /**
     * A Heap-allocated index array
     */
    public static class HeapArray implements IndexArray {
        private final int[] hashes;
        private final long[] offsets;
        
        public HeapArray(int size) {
            logger.debug("Creating heap index of size " + size);
            this.hashes = new int[size];
            this.offsets = new long[size];
        }
        
        public int hash(int slot) {
            return this.hashes[slot];
        }
        
        public void hash(int slot, int hash) {
            this.hashes[slot] = hash;
        }
        
        public long offset(int slot) {
            return this.offsets[slot];
        }
        
        public void offset(int slot, long offset) {
            this.offsets[slot] = offset;
        }
        
        public void set(int slot, int hash, long offset) {
            this.hashes[slot] = hash;
            this.offsets[slot] = offset;
        }
        
        public int capacity() {
            return this.hashes.length;
        }
        
        public void close() {}
    }
    
    /*
     * An array stored in the form (hash, offset)...
     */
    public static class ByteBufferArray implements IndexArray {
        
        private static final int MAX_CHUNK_SIZE = 1024*1024*1024;
        private static final int ELEMENT_SIZE = 12;
        private static final int MAX_ELEMENTS_PER_CHUNK = MAX_CHUNK_SIZE / ELEMENT_SIZE;
        
        private final int capacity;
        private final ByteBuffer[] buffers;
        
        public ByteBufferArray(int size) {
            this.capacity = size;
            int chunks = (int) (ELEMENT_SIZE * (long) size / MAX_CHUNK_SIZE) + 1;
            int lastChunkSize = (size % (MAX_ELEMENTS_PER_CHUNK)) * ELEMENT_SIZE;
            this.buffers = new ByteBuffer[chunks];
            for(int i = 0; i < this.buffers.length - 1; i++)
                this.buffers[i] = ByteBuffer.allocateDirect(MAX_CHUNK_SIZE);
            logger.debug("Allocating " + (this.buffers.length - 1) + " index buffers of size " + MAX_CHUNK_SIZE + " and one buffer of size " + lastChunkSize);
            this.buffers[buffers.length-1] = ByteBuffer.allocateDirect(lastChunkSize);
        }
        
        private int chunk(int slot) {
            return slot / MAX_ELEMENTS_PER_CHUNK;
        }
        
        private int indexInChunk(int slot) {
            return (slot % MAX_ELEMENTS_PER_CHUNK) * ELEMENT_SIZE;
        }

        public long offset(int slot) {
            return this.buffers[chunk(slot)].getLong(indexInChunk(slot) + 4);
        }

        public void offset(int slot, long offset) {
            this.buffers[chunk(slot)].putLong(indexInChunk(slot) + 4, offset);
        }

        public int hash(int slot) {
            return this.buffers[chunk(slot)].getInt(indexInChunk(slot));
        }

        public void hash(int slot, int hash) {
            this.buffers[chunk(slot)].putInt(indexInChunk(slot), hash);
        }

        public void set(int slot, int hash, long offset) {
            hash(slot, hash);
            offset(slot, offset);
        }

        public int capacity() {
            return capacity;
        }  
        
        public void finalize() {
            logger.debug("Garbage collecting " + this.buffers.length + " chunk index.");
        }
        
        public void close() {
            try {
                logger.debug("Attempting to force-free direct buffers.");
                for(ByteBuffer buffer: this.buffers) {
                    Method cleanerMethod = buffer.getClass().getMethod("cleaner");
                    cleanerMethod.setAccessible(true);
                    Object cleaner = cleanerMethod.invoke(buffer);
                    Method cleanMethod = cleaner.getClass().getMethod("clean");
                    cleanMethod.setAccessible(true);
                    cleanMethod.invoke(cleaner);
                }
            } catch(Exception e) {
                logger.debug("Attempt to force de-allocation of the byte buffer failed.", e);
            }
        }
    }

    /**
     * This class represents the index in which we store the mapping of key=>log_position.
     * All public methods should handle their own synchronization.
     */
    private class Index {

        private final HashFunction hashFun;
        private final ReentrantLock lock;
        private int size;
        private IndexArray array;
        private long collisions;

        public Index(boolean singleThreaded, boolean heapAllocate) {
            this.size = 0;
            this.hashFun = Hashing.murmur3_32();
            this.array = allocateIndexArray(config.indexInitialCapacity());
            if(singleThreaded)
                this.lock = new FakeLock();
            else
                this.lock = new ReentrantLock();
        }
        
        private IndexArray allocateIndexArray(int size) {
            if(config.heapAllocateIndex())
                return new HeapArray(size);
            else
                return new ByteBufferArray(size);
        }
        
        private int slot(int hash, int capacity) {
            // use & instead of Math.abs to handle Integer.MIN_VALUE properly
            return (hash & 0x7fffffff) % capacity;
        }

        /**
         * Find the slot where this key/offset is hashed to, if any
         * @param key The key to check
         * @param offset The offset of the value in the log
         * @return the slot in which this key=>offset is stored or -1 if it is not present
         */
        public int slotHolding(ByteBuffer key, long offset) {
            int h = hash(key);
            lock.lock();
            try {
                int slot = slot(h, capacity());
                long foundOffset = array.offset(slot);
                while(foundOffset > 0) {
                    if(array.hash(slot) == h && foundOffset == offset)
                        return slot;
                    slot = nextSlot(slot);
                    foundOffset = array.offset(slot);
                }
                return -1;
            } finally {
                lock.unlock();
            }
        }

        /**
         * Hash the given byte buffer
         */
        public int hash(ByteBuffer bytes) {
            int limit = bytes.limit();
            Hasher hasher = hashFun.newHasher();
            for(int i = 0; i < limit; i++)
                hasher.putByte(bytes.get(i));
            return hasher.hash().asInt();
        }
        
        /**
         * Put the given key=>offset into the index and return the index ref
         * @param key The key
         * @return The index ref for the update
         */
        public IndexSlotSnapshot put(ByteBuffer key, long offset) throws IOException {
            lock.lock();
            try {
                IndexSlotSnapshot entry = find(key, hash(key), lock, true);
                set(entry.slot(), entry.hash(), offset);
                return entry;
            } finally {
                if(lock.isHeldByCurrentThread())
                    lock.unlock();
            }
        }
        
        /**
         * Delete the given key from the index and returns its index reference
         * @param key The key
         * @return The index reference for the entry
         */
        public IndexSlotSnapshot delete(ByteBuffer key) throws IOException {
            lock.lock();
            try {
                IndexSlotSnapshot entry = find(key, hash(key), lock, true);
                // there may be nothing to delete
                if(entry.empty())
                    return entry;
                deleteEntry(entry);
                return entry;
            } finally {
                if(lock.isHeldByCurrentThread())
                    lock.unlock();
            }
        }
        
        /**
         * Get the record associated with this key
         * @param key The key
         * @return The record associated with the key
         */
        public IndexSlotSnapshot get(ByteBuffer key) throws IOException {
            lock.lock();
            try {
                return find(key, hash(key), lock, false);
            } finally {
                if(lock.isHeldByCurrentThread())
                    lock.unlock();
            }
        }
        
        /**
         * The number of valid records in the store
         */
        public int size() {
            lock.lock();
            try {
                return this.size;
            } finally {
                lock.unlock();
            }
        }
        
        public long collisions() {
            lock.lock();
            try {
                return this.collisions;
            } finally {
                lock.unlock();
            }
        }
        
        /**
         * Update the given slot to the new offset iff the current offset equals the given current offset
         * @param slot The slot to update
         * @param offsetExpected The offset we expect in this slot
         * @param newOffset The new offset to set
         * @return true iff the update succeeded
         */
        public boolean testAndSetOffset(int slot, long offsetExpected, long newOffset) {
            lock.lock();
            try {
                if(this.array.offset(slot) == offsetExpected) {
                    this.array.offset(slot, newOffset);
                    return true;
                } else {
                    return false;
                }
            } finally {
                lock.unlock();
            }
        }
        
        /**
         * Find the IndexSlotSnapshot for the given key/hash pair.
         * 
         * This method is a bit of a beast. The complexity is to allow us to avoid re-acquiring the lock
         * if all we want is the record itself.
         * 
         * If an exception is thrown there is no guarantee about the state of the lock.
         * 
         * @param key The key to look for
         * @param hash The hash of the key
         * @param lock The lock to use. It is assumed the lock is already held.
         * @param reacquire Whether or not we should reacquire the lock before returning.
         * @return The index slot snapshot we find.
         */
        private IndexSlotSnapshot find(ByteBuffer key, int hash, ReentrantLock lock, boolean reacquire) throws IOException {
            int slot = slot(hash, capacity());
            IgnoreSet ignore = null;
            while(!vacant(slot)) {
                // if the hashes match, this is a likely hit, check the keys are equal
                if(this.array.hash(slot) == hash && (ignore == null || !ignore.contains(this.array.offset(slot)))) {
                    long offset = this.array.offset(slot);
                    // release the lock before any disk i/o
                    lock.unlock();
                    Record record = log.read(offset, config.checkCrcs());
                    if(record != null && key.equals(record.key())) {
                        // if they requested it, re-acquire the lock and check the validity of the slot we have
                        if(reacquire) {
                            if(!lock.isHeldByCurrentThread()) {
                                lock.lock();
                                // if the offset is invalid now, start over
                                if(offset != this.array.offset(slot)) {
                                    slot = slot(hash, capacity());
                                    continue;
                                }
                            }
                        }
                        assert !reacquire || lock.isHeldByCurrentThread();
                        return new IndexSlotSnapshot(record, slot, hash, offset);
                    } else {
                        // okay, we got a false positive and we released the lock which means there may have been
                        // index changes, so we have to start our search all over again. But we will ignore this offset now.
                        if(ignore == null)
                            ignore = new IgnoreSet(offset);
                        else
                            ignore.add(offset);
                        
                        // reacquire the lock and reset the slot to the beginning
                        lock.lock();
                        slot = slot(hash, capacity());
                        collisions += 1;
                    }
                } else {
                    slot = nextSlot(slot);
                }
                // post-condition: we hold the lock
            }
            assert lock.isHeldByCurrentThread();
            return new IndexSlotSnapshot(null, slot, hash, -1);
        }
        
        

        /**
         * Update the hash and offset based on the values in the given index ref
         */
        private void set(int slot, int hash, long offset) {
            if(logger.isTraceEnabled())
                logger.trace("Setting index entry for slot " + slot + " hash = "
                             + hash + " offset = " + offset);
            // okay its still valid, update it
            boolean insert = vacant(slot);
            this.array.set(slot, hash, offset);
            if(insert) {
                this.size++;
                // expand the index size, if needed
                if(load() > config.indexLoadFactor())
                    expand();
            }
        }

        /**
         * Delete the entry at the given slot
         */
        private void deleteEntry(IndexSlotSnapshot entry) {
            // okay, its valid, delete it
            this.array.set(entry.slot(), 0, 0);
            this.size--;
            // reshuffle all successive items to the left
            for(int curr = nextSlot(entry.slot()); !vacant(curr); curr = nextSlot(curr)) {
                int h = this.array.hash(curr);
                long o = this.array.offset(curr);
                this.array.set(curr, 0, 0);
                int newSlot = nextOpenSlot(h);
                this.array.set(newSlot, h, o);
            }
        }
        
        /**
         * Expand the index by the indexExpansionFactor and rehash all the keys
         */
        private void expand() {
            long start = System.nanoTime();
            int newCapacity = (int) (capacity() * config.indexExpansionFactor());
            if(newCapacity < 0)
                throw new IllegalStateException("Exceeded maximum index slots of "
                                                + Integer.MAX_VALUE);
            IndexArray newArray = allocateIndexArray(newCapacity);
            for(int i = 0; i < capacity(); i++) {
                int h = this.array.hash(i);
                int slot = slot(h, newCapacity);
                while(newArray.offset(slot) != 0)
                    slot = nextSlot(slot, newCapacity);
                newArray.set(slot, this.array.hash(i), this.array.offset(i));
            }

            if(logger.isDebugEnabled()) {
                long ellapsed = (System.nanoTime() - start) / 1000L;
                logger.debug(String.format("Index resized from %d to %d in %d us.",
                                           this.array.capacity(),
                                           newCapacity,
                                           ellapsed));
            }
            this.array.close();
            this.array = newArray;
        }
        
        /** The size of the hash array used for look-ups */
        private int capacity() {
            return array.capacity();
        }

        /** Is there an entry in this slot */
        private boolean vacant(int slot) {
            return this.array.offset(slot) == 0;
        }

        /**
         * Return the next slot after this one
         */
        private int nextSlot(int slot) {
            return nextSlot(slot, capacity());
        }
        
        /**
         * Return the next slot after this one
         */
        private int nextSlot(int slot, int capacity) {
            return (slot + 1) % capacity;
        }

        /**
         * Return the next vacant slot after this one
         */
        private int nextOpenSlot(int hash) {
            int slot = slot(hash, capacity());
            while(!vacant(slot))
                slot = nextSlot(slot);
            return slot;
        }
        
        /**
         * A number between 0 and 1.0 giving the fraction full the hash index is
         */
        private double load() {
            return size / (double) capacity();
        }
        
        /**
         * Load the index from a file
         * @param dir The directory in which to save it
         */
        void readFrom(File file) throws IOException {
            File crcFile = new File(file.getAbsolutePath() + ".crc");
            if(!crcFile.exists())
                throw new IOException("No CRC file found.");
            Crc32 crc = new Crc32();
            CheckedInputStream checksumStream = new CheckedInputStream(new FileInputStream(file), crc);
            DataInputStream input = new DataInputStream(new BufferedInputStream(checksumStream, 64*1024));
            lock.lock();
            try {
                int capacity = input.readInt();
                for(int i = 0; i < capacity; i++) {
                    this.array.hash(i, input.readInt());
                    this.array.offset(i, input.readLong());
                }
                long crcComputed = checksumStream.getChecksum().getValue();
                input.close();
                DataInputStream crcInput = new DataInputStream(new FileInputStream(crcFile));
                long crcExpected = crcInput.readLong();
                crcInput.close();
                
                if(crcComputed != crcExpected)
                    throw new IOException("Index is corrupted (expected crc = " + crcExpected + " but found crc = " + crcComputed + ").");
                
                file.delete();
                crcFile.delete();
            } finally {
                lock.unlock();
            }
        }
        
        void saveTo(File file) throws IOException {
            File crcFile = new File(file.getAbsolutePath() + ".crc");
            file.delete();
            crcFile.delete();
            Crc32 crc = new Crc32();
            CheckedOutputStream checksumStream = new CheckedOutputStream(new FileOutputStream(file), crc);
            DataOutputStream output = new DataOutputStream(new BufferedOutputStream(checksumStream, 64*1024));
            lock.lock();
            try {
                int capacity = this.capacity();
                output.writeInt(capacity);
                for(int i = 0; i < capacity; i++) {
                    output.writeInt(this.array.hash(i));
                    output.writeLong(this.array.offset(i));
                }
                output.flush();
                output.close();
                DataOutputStream crcOutput = new DataOutputStream(new FileOutputStream(crcFile));
                crcOutput.writeLong(checksumStream.getChecksum().getValue());
                crcOutput.flush();
                crcOutput.close();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * A snapshot of the values in an index slot
     */
    private static class IndexSlotSnapshot {

        private final Record record;
        private final int slot;
        private final int hash;
        private final long offset;

        public IndexSlotSnapshot(Record record, int slot, int hash, long offset) {
            this.record = record;
            this.slot = slot;
            this.hash = hash;
            this.offset = offset;
        }

        public boolean empty() {
            return record == null;
        }

        public Record record() {
            return record;
        }

        public int slot() {
            return slot;
        }

        public int hash() {
            return hash;
        }
        
        public long offset() {
            return offset;
        }
    }
    
    private static class IgnoreSet {
        int size;
        long[] offsets;
        
        public IgnoreSet(long offset) {
            this.offsets = new long[1];
            this.offsets[0] = offset;
            this.size = 1;
        }
        
        public boolean contains(long offset) {
            for(int i = 0; i < offsets.length; i++)
                if(offsets[i] == offset)
                    return true;
            return false;
        }
        
        public void add(long offset) {
            if(size == offsets.length) {
                long[] off = new long[offsets.length * 2];
                for(int i = 0; i < offsets.length; i++)
                    off[i] = offsets[i];
                this.offsets = off;
            }
            this.offsets[size] = offset;
            size++;
        }
    }

}