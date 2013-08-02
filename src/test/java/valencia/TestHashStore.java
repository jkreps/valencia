package valencia;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import valencia.utils.ClosableIterator;
import valencia.utils.Utils;
import static org.junit.Assert.*;
import com.google.common.io.*;
import java.io.*;
import java.nio.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class TestHashStore {

    File dir = null;
    HashStoreConfig config = null;
    HashStore store = null;
    ByteBuffer key = ByteBuffer.wrap("key".getBytes());
    ByteBuffer value = ByteBuffer.wrap("value".getBytes());
    ByteBuffer value2 = ByteBuffer.wrap("value2".getBytes());
    Record record = new Record(key.array(), value.array());
    Record record2 = new Record(key.array(), value.array());
    
    public TestHashStore(HashStoreConfig config) {
        this.config = config;
    }
    
    @Parameters
    public static List<Object[]> data() {
      List<Object[]> params = new ArrayList<Object[]>();
      for(boolean singleThreaded: new boolean[]{true, false}) {
          for(boolean heapAllocated: new boolean[]{true, false}) {
              for(boolean saveIndex: new boolean[]{true, false}) {
                  File dir = Files.createTempDir();
                  HashStoreConfig config = new HashStoreConfig(dir)
                                               .indexInitialCapacity(32)
                                               .segmentSize(50)
                                               .checkCrcs(true)
                                               .heapAllocateIndex(heapAllocated)
                                               .singleThreaded(singleThreaded)
                                               .segmentDeletionDelay(0, TimeUnit.MILLISECONDS)
                                               .saveIndexOnClose(saveIndex);
                  params.add(new Object[]{config});
              }
          }
      }
      return params;
    }

    @Before
    public void setup() throws IOException {
        this.store = new HashStore(config);
    }

    @After
    public void teardown() {
        this.store.close();
        Utils.delete(this.config.directory());
    }

    @Test
    public void testGetNonExistant() throws IOException {
        assertNull(this.store.get(key));
    }

    @Test
    public void testDeleteNonExistant() throws IOException {
        assertNull(this.store.get(key));
    }

    @Test
    public void testPutGetDelete() throws IOException {
        assertEquals("Empty store", 0, store.count());
        assertNull("Should find no previous record", store.put(record));
        assertEquals("Empty store", 1, store.count());
        assertEquals("Should find the record by key.", record, this.store.get(key));
        assertEquals("Should delete the record and return previous", record, this.store.delete(key));
        assertEquals("Empty store", 0, store.count());
        assertNull("Should not find the record by key.", this.store.get(key));
    }

    @Test
    public void testOverwrite() throws IOException {
        assertNull("Should find no previous record", store.put(record));
        assertEquals("Should find the record by key.", record, this.store.get(key));
        assertEquals("Should find the record by key.", record, this.store.put(record2));
        assertEquals("Should find the record by key.", record2, this.store.get(key));
    }

    @Test
    public void testIndexExpansion() throws IOException {
        int numRecords = 3 * this.store.config().indexInitialCapacity();
        List<Record> records = StoreTestUtils.randomRecords(numRecords);
        for(int i = 0; i < numRecords; i++) {
            assertEquals(i, this.store.count());
            assertNull(this.store.put(records.get(i)));
            assertNotNull(this.store.get(records.get(i).key()));
        }
        for(Record record: records) {
            assertEquals("Found record does not match put record.",
                         record,
                         this.store.get(record.key()));
        }
    }

    @Test
    public void testGc() throws IOException {
        // TODO: need to ensure the gc happens before the assertion
        int numRecords = this.store.config().segmentSize();
        List<Record> records = StoreTestUtils.randomRecords(numRecords);
        // write all the records a few times to incur some garbage
        for(int i = 0; i < 5; i++)
            putAll(records);
        for(Record record: records)
            assertEquals("Found record does not match put record.",
                         record,
                         this.store.get(record.key()));
    }
    
    @Test
    public void testIteration() throws IOException {
        int numRecords = this.store.config().segmentSize();
        List<Record> records = StoreTestUtils.randomRecords(numRecords);
        byte[] key = "hello".getBytes();
        Record old = new Record(key, "world".getBytes());
        records.add(old);
        putAll(records);
        assertEquals("Iteration should give back what we put", records, records(this.store));
        
        // now update one record and check it is updated in the iterator
        Record newer = new Record(key, "there".getBytes());
        this.store.put(newer);
        records.set(records.size() - 1, newer);
        assertEquals("Iteration should give back what we put", records, records(this.store));
    }
    
    @Test
    public void testNormalRecovery() throws IOException {
        List<Record> records = StoreTestUtils.randomRecords(10);
        putAll(records);
        this.store.close();
        this.store = new HashStore(config);
        assertEquals("Same records should be present after close and re-open", records, records(store));
    }
    
    @Test
    public void testRecoveryTotalCorruption() throws IOException {
        List<Record> records = StoreTestUtils.randomRecords(10);
        putAll(records);
        // mangle log file
        LogSegment seg = this.store.log().segmentFor(0);
        writeToOffset(seg.file(), 0, "Hayduke lives!".getBytes());
        this.store.close();
        this.store = new HashStore(config);
        if(!config.saveIndexOnClose())
            assertEquals("No records should be present after mangling", Collections.emptyList(), records(store));
    }
    
    @Test
    public void testRecoveryBadMessageLength() throws IOException {
        List<Record> records = StoreTestUtils.randomRecords(10);
        putAll(records);
        
        // now test a bunch of bad message lengths
        testRecoveryWithBadMessageSize(records, 0);
        //testRecoveryWithBadMessageSize(records, Integer.MAX_VALUE);
        testRecoveryWithBadMessageSize(records, 5);
    }
    
    private void testRecoveryWithBadMessageSize(List<Record> records, int size) throws IOException {
        LogSegment seg = this.store.log().lastSegment();
        writeToOffset(seg.file(), seg.file().length(), ByteBuffer.allocate(4).putInt(size).array());
        // now add some message bytes, but not enough
        if(size > 0)
            writeToOffset(seg.file(), seg.file().length(), ByteBuffer.allocate(Math.min(size-1, 256)).array());
        this.store.close();
        this.store = new HashStore(config);
        assertEquals("Same records should be present after close and re-open", records, records(store));
    }
    
    @Test
    public void testRecoveryWithAdditionalEmptyFile() throws IOException {
        List<Record> records = StoreTestUtils.randomRecords(10);
        putAll(records);
        this.store.close();
        // recreate without closing, we should have trailing empty bytes on the end of the log
        this.store = new HashStore(config);
        assertEquals("Same records should be present after close and re-open", records, records(store));
    }
    
    @Test
    public void testRecoveryCorruptMessage() throws IOException {
        List<Record> records = StoreTestUtils.randomRecords(10);
        putAll(records);
        
        // append a message with a crc that won't possibly validate
        Record invalid = new Record(ByteBuffer.wrap(StoreTestUtils.randomBytes(10)));
        store.log().append(invalid);
        this.store.close();
        this.store = new HashStore(config);
        //assertEquals("Same records should be present after close and re-open", records, records(store));
    }
    
    private void writeToOffset(File file, long offset, byte[] bytes) throws IOException {
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        f.seek(offset);
        f.write(bytes);
        f.close();
    }
    
    private void putAll(List<Record> records) throws IOException {
        for(Record r: records) {
            this.store.put(r);
            assertEquals("Any record we put should be readable.", r, this.store.get(r.key()));
        }
    }
    
    private List<Record> records(HashStore store) {
        ClosableIterator<Record> iter = store.iterator();
        List<Record> l = new ArrayList<Record>();
        while(iter.hasNext())
            l.add(iter.next());
        iter.close();
        return l;
    }
    
}