package valencia;

import java.nio.*;

import valencia.utils.Crc32;

/**
 * An N byte binary record in the following format: 4 byte crc 4 byte key size
 * containing k, the length of the key k byte key N-k-8 byte value value
 * 
 */
class Record {

    private final ByteBuffer buffer;

    Record(ByteBuffer buffer) {
        this.buffer = buffer;
    }
    
    public Record(ByteBuffer key, ByteBuffer value) {
        this(ByteBuffer.allocate(8 + key.remaining() + value.remaining()));
        buffer.position(4);
        buffer.putInt(key.limit());
        key.mark();
        buffer.put(key);
        key.reset();
        value.mark();
        buffer.put(value);
        value.reset();
        buffer.position(0);
        buffer.putInt((int) (contentCrc() & 0xffffffffL));
        buffer.rewind();
    }

    public Record(byte[] key, byte[] value) {
        this(ByteBuffer.allocate(8 + key.length + value.length));
        buffer.position(4);
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.put(value);
        buffer.position(0);
        buffer.putInt((int) (contentCrc() & 0xffffffffL));
        buffer.rewind();
    }

    public ByteBuffer buffer() {
        return this.buffer;
    }

    public int size() {
        return buffer.limit();
    }

    public int keySize() {
        return buffer.getInt(4);
    }

    public int valueSize() {
        return size() - keySize() - 8;
    }

    public long storedCrc() {
        return buffer.getInt(0) & 0xffffffffL;
    }

    public long contentCrc() {
        Crc32 crc = new Crc32();
        for(int i = 4; i < buffer.limit(); i++)
            crc.update(buffer.get(i));
        return crc.getValue();
    }

    public ByteBuffer key() {
        ByteBuffer kb = (ByteBuffer) buffer.asReadOnlyBuffer().position(8);
        return (ByteBuffer) kb.slice().limit(keySize());
    }

    public ByteBuffer value() {
        ByteBuffer vb = (ByteBuffer) buffer.asReadOnlyBuffer().position(8 + keySize());
        return (ByteBuffer) vb.slice().limit(valueSize());
    }

    public void validate() {
        if(storedCrc() != contentCrc())
            throw new ChecksumFailureException("Expected crc " + storedCrc()
                                            + ", but actual crc is " + contentCrc() + ".");
    }

    @Override
    public String toString() {
        return "Record(crc = " + storedCrc() + ", key_length = " + keySize() + ", value_length = "
               + valueSize() + ", [key], [value])";
    }

    public boolean equals(Object o) {
        if(o == null)
            return false;
        if(this == o)
            return true;
        if(!(o instanceof Record))
            return false;
        Record r = (Record) o;
        return r.buffer.equals(this.buffer);
    }

    public int hashCode() {
        return this.buffer.hashCode();
    }

}