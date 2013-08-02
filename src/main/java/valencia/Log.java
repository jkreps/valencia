package valencia;

import java.io.*;
import java.util.Map.Entry;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.log4j.*;

import valencia.utils.ClosableIterator;

/**
 * An append only log that supports offset-based lookup of records.
 * 
 * The last segment in the log is always a FileLogSegment; all other segments are MappedSegments.
 */
class Log {

    public static final String FILE_SUFFIX = ".log";
    private static final String FILE_NAME_FORMAT = "%020d" + FILE_SUFFIX;
    private static final Logger logger = Logger.getLogger(Log.class);

    private final ConcurrentNavigableMap<Long, LogSegment> segs;
    private final File directory;
    private final int segmentSize;
    private final int maxRecordSize;

    public Log(File directory, int maxRecordSize, int segmentSize) throws IOException {
        this.segs = new ConcurrentSkipListMap<Long, LogSegment>();
        this.directory = directory;
        this.segmentSize = segmentSize;
        this.maxRecordSize = maxRecordSize;
        init();
    }

    /**
     * Initialize the log
     */
    private void init() throws IOException {
        // create the log directory if it doesn't exist
        if(!directory.exists()) {
            boolean success = directory.mkdirs();
            if(!success)
                throw new IOException("Could not create log directory "
                                      + directory.getAbsolutePath());
        }
        // load all the log segments
        File[] files = directory.listFiles(new FileFilter() {
            public boolean accept(File path) {
                return path.getName().endsWith(FILE_SUFFIX);
            }
        });
        
        for(int i = 0; i < files.length; i++) {
            int lastSegment = files.length -1;
            boolean mutable = i == lastSegment;
            LogSegment s = new LogSegment(files[i], this.maxRecordSize, mutable);
            segs.put(s.baseOffset(), s);
        }
        if(segs.size() == 0)
            initEmptyLog();
    }
    
    private void initEmptyLog() throws IOException {
        File file = new File(directory, String.format(Log.FILE_NAME_FORMAT, 0));
        segs.put(0L, LogSegment.create(file, this.maxRecordSize, segmentSize));
    }
    
    /**
     * Truncate this log to the given offset, discarding any segments with a base offset larger than
     * the given offset and shortening the last segment to begin at the given offset. This offset must be a valid
     * record offset or else something bad will happen.
     * 
     */
    public synchronized long truncateTo(long offset) throws IOException {
        long truncated = 0;
        // First delete any trailing segments
        if(lastSegment().baseOffset() > offset) {
            logger.warn("Truncating invalid segments");
            for(LogSegment segment = lastSegment(); segment != null && segment.baseOffset() > offset; segment = lastSegment()) {
                truncated += segment.size();                
                delete(segment, true);
            }
        }
        if(segmentCount() == 0) {
            // no data left!
            initEmptyLog();
        } else {
            // otherwise cut down the last segment to the right size.
            LogSegment last = lastSegment();
            truncated += last.truncateTo((int) (offset - last.baseOffset()));
        }
        if(truncated > 0)
            logger.warn("Truncated " + truncated + " from log.");
        return truncated;
    }

    /**
     * Append the record to the end of the log and return its offset.
     * Only one thread can append to the log at any given time.
     */
    public synchronized long append(Record record) throws IOException {
        LogSegment segment = lastSegment();
        if(segment.size() + record.size() + 4 > segmentSize)
            segment = roll();
        return segment.append(record);
    }

    /**
     * Get the record stored at this offset
     */
    public Record read(long offset, boolean validate) throws IOException {
        LogSegment segment = segmentFor(offset);
        if(segment == null)
            return null;
        if(logger.isTraceEnabled())
            logger.trace("Reading record from offset " + offset + " from segment "
                         + segment.baseOffset());
        return segment.read(offset, validate);
    }

    /**
     * Roll over to a new, empty active segment
     * 
     * @param segment The segment to roll over
     * @return The new log segment
     */
    synchronized LogSegment roll() throws IOException {
        LogSegment segment = lastSegment();
        long start = System.nanoTime();
        segment.readOnly();
        segment.truncateExcess();
        long baseOffset = segment.baseOffset() + segment.size();
        File newFile = new File(this.directory, String.format(Log.FILE_NAME_FORMAT, baseOffset));
        LogSegment newSegment = LogSegment.create(newFile, this.maxRecordSize, segmentSize);
        segs.put(baseOffset, newSegment);
        double ellapsed = (System.nanoTime() - start)/(1000.0*1000.0);
        if(logger.isDebugEnabled())
            logger.debug(String.format("Rolled over segment %s in %.3f ms.", segment.file().getName(), ellapsed));
        return newSegment;
    }

    /**
     * Delete the given log segment
     * @param segment The segment to delete
     * @param force If true with will ignore any outstanding references to the segment
     */
    public void delete(LogSegment segment, boolean force) {
        if(!force && segment.hasReferences())
            throw new IllegalStateException("Attempt to delete segment " + segment.file().getName() + " which still has active references.");
        segs.remove(segment.baseOffset());
        segment.delete();
    }

    /**
     * The segments in this log
     */
    public Iterable<LogSegment> segments() {
        return segs.values();
    }

    /**
     * Get the segment containing the given offset
     * 
     * @param offset The offset to search for
     * @return The segment that contains this offset or null if it is out of the
     *         range of the log
     */
    public LogSegment segmentFor(long offset) {
        Entry<Long, LogSegment> entry = segs.floorEntry(offset);
        if(entry == null)
            return null;
        else
            return entry.getValue();
    }

    /**
     * Get the last segment
     */
    LogSegment lastSegment() {
        Entry<Long, LogSegment> last = segs.lastEntry();
        if(last == null)
            return null;
        else
        return last.getValue();
    }

    /**
     * @return The directory in which this log resides
     */
    public File directory() {
        return this.directory;
    }

    /**
     * Close this log
     */
    public void close() {
        for(LogSegment segment: segments())
            segment.close();
    }
    
    public double utilization() {
        double live = 0, total = 0;
        for(LogSegment segment: segments()) {
            live += segment.liveRecords();
            total += segment.totalRecords();
        }
        return live/total;
    }

    /**
     * The total length of all log segments in bytes
     */
    public long length() {
        long total = 0;
        for(LogSegment s: segments())
            total += s.size();
        return total;
    }

    /**
     * The number of log segments
     */
    public int segmentCount() {
        return segs.size();
    }
    
    /**
     * Mark record deleted
     */
    public void markDeleted(long offset) {
        LogSegment segment = segmentFor(offset);
        // if the segment is null it has already been garbage collected, ignore
        if(segment != null)
            segment.recordRecordDeletion();
    }
    
    /**
     * An iterator over the records in this log segment
     */
    public ClosableIterator<LogEntry> iterator() {
        return iterator(false);
    }
    
    public ClosableIterator<LogEntry> iterator(boolean validate) {
        return new LogIterator(validate);
    }
    
    
    private class LogIterator implements ClosableIterator<LogEntry> {

        private final boolean validate;
        private LogSegment segment;
        private ClosableIterator<LogEntry> iter;
        private LogEntry entry;
        
        public LogIterator(boolean validate) {
            this.validate = validate;
            this.segment = segs.firstEntry().getValue();
            this.iter = segment.iterator(validate);
        }

        public boolean hasNext() {
            return makeNext();
        }

        public LogEntry next() {
            if(makeNext()) {
                LogEntry entry = this.entry;
                this.entry = null;
                return entry;
            } else {
                throw new NoSuchElementException();
            }
        }
        
        /* create the next item and return true, or return false if exhausted */
        private boolean makeNext() {
            // we already have an entry ready to go, use it
            if(this.entry != null)
                return true;
            // make a new entry
            if(this.iter.hasNext()) {
                this.entry = this.iter.next();
                return true;
            }
            
            // the current segment is exhausted, see if there is another
            this.iter.close();
            Map.Entry<Long, LogSegment> entry = segs.higherEntry(this.segment.baseOffset());
            if(entry == null)
                return false;
            this.segment = entry.getValue();
            this.iter = this.segment.iterator(validate);
            return makeNext();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
        
        public void close() {
            iter.close();
        }
    };
    
}
