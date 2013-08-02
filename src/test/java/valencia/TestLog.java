package valencia;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.*;

import com.google.common.io.Files;

import valencia.utils.ClosableIterator;
import valencia.utils.Utils;
import static org.junit.Assert.*;

public class TestLog {

    private Log log;

    public TestLog() {}

    @Before
    public void setup() throws IOException {
        int segmentSize = 100;
        log = new Log(Files.createTempDir(), segmentSize, segmentSize);
    }

    @After
    public void tearDown() {
        Utils.delete(log.directory());
    }

    @Test
    public void testReadAndWrite() throws IOException {
        List<Record> records = new ArrayList<Record>();
        List<Long> offsets = new ArrayList<Long>();
        for(int i = 0; i < 10; i++) {
            // write some records, and record their offsets
            List<Record> currRecords = StoreTestUtils.randomRecords(10);
            records.addAll(currRecords);
            for(Record r: currRecords)
                offsets.add(log.append(r));

            // we should be able to read the records we wrote at the offsets we
            // got
            List<Record> found = new ArrayList<Record>();
            for(long offset: offsets)
                found.add(log.read(offset, true));
            assertEquals("Should be able to read all written records on iteration " + i,
                         records,
                         found);

            // check that iteration gives the segments in order by base offset
            Iterator<LogSegment> iter = log.segments().iterator();
            LogSegment prev = null;
            LogSegment curr = iter.next();
            while(iter.hasNext()) {
                prev = curr;
                curr = iter.next();
                assertTrue("Segments should be ordered by base offset",
                           prev.baseOffset() < curr.baseOffset());
            }
        }
        
        // test iteration over the full log
        ClosableIterator<LogEntry> logIter = log.iterator();
        Iterator<Record> iter = records.iterator();
        while(logIter.hasNext() || iter.hasNext())
            assertEquals("The records should be equal", iter.next(), logIter.next().record());

        logIter.close();
    }

}