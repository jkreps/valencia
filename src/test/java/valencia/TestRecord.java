package valencia;

import org.junit.*;
import static org.junit.Assert.*;
import java.nio.*;

public class TestRecord {

    byte[] key = "key".getBytes();
    byte[] value = "value".getBytes();

    public TestRecord() {}

    @Test
    public void testSerialization() {
        Record record = new Record(key, value);
        record.validate();
        assertEquals(key.length, record.keySize());
        assertEquals(value.length, record.valueSize());
        assertEquals(ByteBuffer.wrap(key), record.key());
        assertEquals(ByteBuffer.wrap(value), record.value());
        assertEquals(8 + key.length + value.length, record.size());

        // check parameters of the byte buffers returned by the getters
        assertEquals(0, record.key().position());
        assertEquals(key.length, record.key().limit());
        assertEquals(0, record.value().position());
        assertEquals(value.length, record.value().limit());
    }
}