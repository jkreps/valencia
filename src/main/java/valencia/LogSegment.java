package valencia;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import valencia.utils.ClosableIterator;

/**
 * A segment in the log.
 * 
 * Segments are pre-allocated to a given max size as a sparse file. This file is then memory mapped for reads and writes.
 * When the segment is closed we truncate off any excess bytes.
 */
public class LogSegment {

    private static final Logger logger = Logger.getLogger(LogSegment.class);
    private static final byte[] header;
    
    static {
        try{
            header = "v0".getBytes("UTF-8");
        } catch(UnsupportedEncodingException e) {
            throw new IllegalStateException("This cannot happen.", e);
        }
    }

    private final File file;
    private final AtomicBoolean valid;
    private final MappedByteBuffer buffer;
    private final long baseOffset;
    private final AtomicInteger validLength;
    private final AtomicBoolean mutable;
    private final AtomicInteger references;
    private final AtomicInteger liveRecords;
    private final AtomicInteger records;
    private final int maxRecordSize;

    /**
     * Creates a LogSegment from the give file. The file must exist.
     * @param fileName The name of the file
     * @param mutable Whether the segment should be mutable
     */
    LogSegment(File fileName, int maxRecordSize, boolean mutable) throws IOException {
        if(!fileName.exists())
            throw new IllegalArgumentException("Attempt to open non-existant file " + fileName.getAbsolutePath() + " as immutable segment.");
        if(fileName.length() > Integer.MAX_VALUE)
            throw new IllegalArgumentException("File is " + fileName.length()
                                               + " larger than maximum allowable size ("
                                               + Integer.MAX_VALUE + ")");
        
        this.file = fileName;
        this.mutable = new AtomicBoolean(mutable);
        this.baseOffset = Long.parseLong(fileName.getName()
                                             .substring(0,
                                                        fileName.getName().length()
                                                                - Log.FILE_SUFFIX.length()));
        this.liveRecords = new AtomicInteger(0);
        this.records = new AtomicInteger(0);
        this.valid = new AtomicBoolean(true);
        this.references = new AtomicInteger(0); 
        this.maxRecordSize = maxRecordSize;
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        try {
            this.buffer = raf.getChannel().map(mutable? MapMode.READ_WRITE : MapMode.READ_ONLY, 0, file.length());
            this.validLength = new AtomicInteger((int) file.length());
        } finally {
            raf.close();
        }
    }
    
    /**
     * Check that the header on this file matches our expectation.
     */
    private void validateHeader() {
        ByteBuffer buffer = this.buffer.duplicate();
        if(buffer.limit() < header.length)
            throw new LogCorruptionException("Incomplete header on log file " + this.file.getAbsolutePath() + ".");
        for(int i = 0; i < header.length; i++)
            if(header[i] != buffer.get(i))
                throw new LogCorruptionException("Log header on " + this.file.getAbsolutePath() + " does not match expected value.");
    }
    
    /**
     * Create a new, empty, mutable log segment with the given file name
     * @param file The file name to use
     * @param maxSize The size of the file to pre-allocate
     * @return The new segment
     */
    public static LogSegment create(File file, int maxRecordSize, int maxSegmentSize) throws IOException {
        boolean success = file.createNewFile();
        if(!success)
            throw new IOException("Failed to create segment file " + file.getAbsolutePath());
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.write(header);
        raf.setLength(maxSegmentSize);
        raf.close();
        LogSegment segment = new LogSegment(file, maxRecordSize, true);
        // initialize to begin writing at the first slot after the header
        segment.buffer.position(header.length);
        segment.validLength.set(header.length);
        return segment;
    }

    /**
     * The size of the valid portion of this log in bytes
     */
    public int size() {
        return validLength.get();
    }
    
    /**
     * Record the addition of a new record in the log
     */
    private void recordRecordInsert() {
        this.records.incrementAndGet();
        this.liveRecords.incrementAndGet();
    }
    
    /**
     * The total number of live records in the log
     */
    public int liveRecords() {
        return this.liveRecords.get();
    }
    
    /**
     * The total number of records (active and inactive) in the log
     */
    public int totalRecords() {
        return this.records.get();
    }
    
    /**
     * Record a record deletion for a record in this segment
     */
    public void recordRecordDeletion() {
        this.liveRecords.decrementAndGet();
    }
    
    /**
     * The utilization of this segment
     */
    public double utilization() {
        return liveRecords.get() / (double) records.get();
    }
    
    /**
     * Flush the mapping
     */
    public void close() {
        try {
            flush();
            truncateExcess();
        } catch(IOException e) {
            logger.error("Failed to truncate file while closing log segment.");
        }
    }
    
    /**
     * return true iff the segment is valid.
     */
    public boolean valid() {
        return this.valid.get();
    }
    
    /**
     * The number of references to this log segment
     */
    public boolean hasReferences() {
        return this.references.get() > 0;
    }
    
    /**
     * The file in which this log segment is stored
     */
    public File file() {
        return this.file;
    }
    
    /**
     * Mark the segment as invalid (if it isn't already). Return true if it was updated.
     */
    public boolean invalidate() {
        return this.valid.compareAndSet(true, false);
    }
    
    /**
     * Is this log segment mutable?
     */
    public boolean mutable() {
        return this.mutable.get();
    }
    
    /**
     * Get the base offset at which this segment begins
     */
    public long baseOffset() {
        return this.baseOffset;
    }

    /**
     * Delete this log segment
     */
    public synchronized void delete() {
        if(logger.isDebugEnabled())
            logger.debug("Deleting log segment " + file.getName());
        this.file.delete();
    }
    
    /**
     * Make this segment read-only. All future write attempts will throw an exception
     */
    public void readOnly() throws IOException {
        this.mutable.set(false);
    }
    
    /**
     * Append the given record to the end of the log
     * @param record The record to append
     * @return The (global) log offset of the record
     * @throws IOException
     */
    public synchronized long append(Record record) throws IOException {
        if(!mutable())
            throw new IllegalStateException("Attempt to write to an immutable log segment.");
        if(record.size() > this.maxRecordSize)
            throw new IllegalArgumentException("Attempt to write " + record.size() + " byte record, which exceeds the maximum allowable record size, " + 
                                               " which is configured to " + this.maxRecordSize + ".");
        int localOffset = buffer.position();
        int recordSize = record.size();
        buffer.putInt(recordSize).put(record.buffer());
        this.recordRecordInsert();
        record.buffer().rewind();
        this.validLength.addAndGet(recordSize + 4);
        return baseOffset() + localOffset;
    }
    
    public synchronized int truncateExcess() throws IOException {
        return truncateTo(size());
    }
    
    public synchronized int truncateTo(int size) throws IOException {
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        long length = f.length();
        try {
            int newLength = Math.max(header.length, size);
            f.setLength(newLength);
            this.validLength.set(newLength);
        } finally {
            f.close();
        }
        return (int) (length - size);
    }

    /**
     * Close this log segment
     */
    public synchronized void flush() throws IOException {
        if(logger.isDebugEnabled())
            logger.debug("Flushing log segment " + file.getName());
        buffer.force();
    }
    
    /**
     * Read a record from the given offset
     * @param offset The offset to read from
     * @return The record
     */
    public Record read(long offset, boolean validate) throws IOException {
        int localOffset = (int) (offset - baseOffset());
        int size = size();
        if(localOffset + 4 > size)
            throw new LogCorruptionException("Attempt to read from offset " + localOffset
                                               + " on segment " + file.getAbsolutePath()
                                               + " which has only " + size + " bytes.");
        ByteBuffer ref = buffer.duplicate();
        ref.position(localOffset);
        int recordSize = ref.getInt();
        if(recordSize <= 0)
            throw new LogCorruptionException("Record with size " + recordSize + ".");
        if(localOffset + 4 + recordSize > size)
            throw new LogCorruptionException("Attempt to read " + (size + 4)
                                               + " bytes from offset " + localOffset + " on segment "
                                               + file.getAbsolutePath() + " which has only "
                                               + size + " bytes.");
        if(recordSize > this.maxRecordSize)
            throw new LogCorruptionException("Found a record of size " + recordSize + 
                                             " bytes, larger than the maximum configured record size of " + maxRecordSize + ".");
        ByteBuffer slice = ref.slice();
        slice.limit(recordSize);
        Record record = new Record(slice);
        
        if(validate)
            record.validate();
        
        return record;
    }
    
    /**
     * An iterator over the records in this log segment
     */
    public ClosableIterator<LogEntry> iterator() {
        return iterator(false);
    }

    /**
     * An iterator over the records in this log segment
     * @param validate If true the CRC of the message will be checked on read
     */
    public ClosableIterator<LogEntry> iterator(boolean validate) {
        this.references.getAndIncrement();
        return new LogSegmentIterator(validate);
    }
    
    private class LogSegmentIterator implements ClosableIterator<LogEntry> {
        private final boolean validate;
        private final AtomicBoolean open;
        private long position;
        
        public LogSegmentIterator(boolean validate) {
            this.validate = validate;
            this.open = new AtomicBoolean(true);
            this.position = header.length;
            validateHeader();
        }

        public boolean hasNext() {
            checkOpen();
            return position < LogSegment.this.size();
        }

        public LogEntry next() {
            checkOpen();
            try {
                long here = baseOffset() + position;
                Record r = read(here, validate);
                position += 4 + r.buffer().remaining();
                return new LogEntry(here, r);
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
        
        public void close() {
            if(open.getAndSet(false))
                references.decrementAndGet();
            // if it is already closed, just ignore
        }
        
        private void checkOpen() {
            if(!open.get())
                throw new IllegalStateException("This iterator has already been closed.");
        }
    }

}