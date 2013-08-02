package valencia;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.junit.*;

import valencia.utils.ClosableIterator;
import valencia.utils.Utils;
import static org.junit.Assert.*;

public class TestLogSegment {

    LogSegment segment = null;

    @Before
    public void setup() throws IOException {
        segment = StoreTestUtils.createEmptyLogSegment(4096);
    }

    @After
    public void tearDown() {
        Utils.delete(segment.file().getParentFile());
    }

    @Test
    public void testSegmentDelete() throws IOException {
        segment.append(StoreTestUtils.randomRecord());
        segment.delete();
        assertFalse("File should not exist.", segment.file().exists());
    }

    @Test
    public void testSimpleReadAndWrite() throws IOException {
        List<Record> records = StoreTestUtils.randomRecords(10);
        List<Record> found = readRecords(writeRecords(records));
        assertEquals("Should be able to read all written records.", records, found);
    }
    
    @Test
    public void testCloseAndReopen() throws IOException {
        List<Record> records = StoreTestUtils.randomRecords(10);
        long[] offsets = writeRecords(records);
        LogSegment newSegment = new LogSegment(segment.file(), Integer.MAX_VALUE, false);
        assertEquals("Data should be the same when segment is re-opened", records, readRecords(newSegment, offsets));
    }
    
    @Test(expected = IllegalStateException.class)
    public void testImmutableSegmentIsImmutable() throws IOException {
        segment.readOnly();
        segment.append(StoreTestUtils.randomRecord());
    }
    
    @Test
    public void testSegmentTruncation() throws IOException {
        List<Record> records = StoreTestUtils.randomRecords(3);
        long[] offsets = writeRecords(records);
        assertEquals(3, records(segment).size());
        segment.truncateTo((int) ((offsets[1]-segment.baseOffset()) + 4 + records.get(1).size()));
        ClosableIterator<LogEntry> iter = segment.iterator();
        assertEquals("First record should be equal", records.get(0), iter.next().record());
        assertEquals("Second record should be equal", records.get(1), iter.next().record());
        assertFalse("There should not be a third record", iter.hasNext());
    }
    
    @Test
    public void testIteration() throws IOException {
        List<Record> records = StoreTestUtils.randomRecords(10);
        writeRecords(records);
        
        assertEquals("We should get the same records from the iterator.", records, records(segment));
        
        // should be no references yet
        assertFalse(segment.hasReferences());
                
        // When we create an iterator we should have a reference to the segment
        ClosableIterator<LogEntry> iter = segment.iterator();
        assertTrue(segment.hasReferences());
        
        // Closing the iterator should remove the reference
        iter.close();
        assertFalse(segment.hasReferences());
    }
    
    private List<Record> readRecords(long[] offsets) throws IOException {
        return readRecords(segment, offsets);
    }
    
    private List<Record> readRecords(LogSegment segment, long[] offsets) throws IOException {
        List<Record> found = new ArrayList<Record>();
        for(long offset: offsets)
            found.add(segment.read(offset, true));
        return found;
    }
    
    private long[] writeRecords(List<Record> records) throws IOException {
        // write some records, and record their offsets
        long[] offsets = new long[records.size()];
        for(int i = 0; i < records.size(); i++)
            offsets[i] = segment.append(records.get(i));
        return offsets;
    }

    private List<Record> records(LogSegment segment) {
        List<Record> recs = new ArrayList<Record>();
        ClosableIterator<LogEntry> iter = segment.iterator();
        while(iter.hasNext()) {
            LogEntry entry = iter.next();
            recs.add(entry.record());
        }
        iter.close();
        return recs;
    }

}