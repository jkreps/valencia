package valencia;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class TestLinearWritePerformance {
    
    public static void main(String[] args) throws IOException {
        OptionParser parser = new OptionParser();
        OptionSpec<Integer> maxBufferSizeOpt = 
                parser.accepts("max-buffer-size", "The maximum size of the buffer to use for writing.")
                      .withRequiredArg()
                      .describedAs("size")
                      .ofType(Integer.class)
                      .defaultsTo(512*1024*1024);
        OptionSpec<Integer> maxDataOpt = 
                parser.accepts("max-length", "The maximum amount of data to write.")
                      .withRequiredArg()
                      .describedAs("size")
                      .ofType(Integer.class)
                      .defaultsTo(512*1024*1024);
        OptionSpec<Integer> multiplierOpt = 
                parser.accepts("multiplier", "The mupliplier by which to increase the buffer size between iterations.")
              .withRequiredArg()
              .ofType(Integer.class)
              .defaultsTo(2);
        OptionSet options = parser.parse(args);
        int maxSize = options.valueOf(maxBufferSizeOpt);
        long maxLength = options.valueOf(maxDataOpt);
        int multiplier = options.valueOf(multiplierOpt);
        System.out.println(String.format("%25s %25s %25s %25s %25s %25s %25s", "file_length", "size (bytes)", "raf (mb/sec)", "channel_direct (mb/sec)", "mmap_direct (mb/sec)", "channel_heap (mb/sec)", "mmap_heap (mb/sec)"));
        for(int size = 1; size < maxSize; size *= multiplier) {
            long length = Math.min(maxLength, 1000000L*size);
            System.out.print(String.format("%25d %25d ", length, size));
            long time = testRandomAccessFileWrite(size, length);
            System.out.print(String.format(" %25.2f", mbPerSec(time, length)));
            System.gc();
            for(boolean direct: new boolean[]{true, false}) {
                time = testFileChannelWrite(size, length, direct);
                System.out.print(String.format(" %25.2f", mbPerSec(time, length)));
                System.gc();
                time = testMappedWrite(size, length, direct);
                System.out.print(String.format(" %25.2f", mbPerSec(time, length)));
                System.gc();
            }
            System.out.println();    
        }
    }
    
    private static double mbPerSec(double ns, double bytes) {
        double mbs = bytes / (1024.0*1024.0);
        double secs = ns / 1000.0 / 1000.0 / 1000.0;
        return mbs/secs;
    }
    
    private static long testRandomAccessFileWrite(int size, long length) throws IOException {
        File temp = tempFile();
        RandomAccessFile file = new RandomAccessFile(temp, "rw");
        byte[] bytes = new byte[size];
        long start = System.nanoTime();
        for(int written = 0; written < length; ) {
            file.write(bytes);
            written += bytes.length;
        }
        long ellapsed = (System.nanoTime() - start);
        file.close();
        temp.delete();
        return ellapsed;
    }
    
    private static long testFileChannelWrite(int size, long length, boolean direct) throws IOException {
        File temp = tempFile();
        FileChannel channel = new RandomAccessFile(temp, "rw").getChannel();
        ByteBuffer bytes = ByteBuffer.allocate(size);
        long start = System.nanoTime();
        for(int written = 0; written < length; ) {
            bytes.rewind();
            written += channel.write(bytes);
        }
        long ellapsed = (System.nanoTime() - start);
        channel.close();
        temp.delete();
        return ellapsed;
    }
    
    private static long testMappedWrite(int size, long length, boolean direct) throws IOException {
        File temp = tempFile();
        RandomAccessFile f = new RandomAccessFile(temp, "rw");
        f.setLength(length+size);
        FileChannel channel = f.getChannel();
        ByteBuffer bytes = allocate(size, direct);
        MappedByteBuffer mmap = channel.map(MapMode.READ_WRITE, 0, length + size);
        long start = System.nanoTime();
        for(int written = 0; written < length; ) {
            bytes.rewind();
            mmap.put(bytes);
            written += bytes.position();
        }
        long ellapsed = (System.nanoTime() - start);
        channel.close();
        temp.delete();
        return ellapsed;
    }
    
    private static ByteBuffer allocate(int size, boolean direct) {
        if(direct)
            return ByteBuffer.allocateDirect(size);
        else
            return ByteBuffer.allocate(size);
    }
    
    private static File tempFile() throws IOException {
        File temp = File.createTempFile("test", ".dat");
        temp.deleteOnExit();
        return temp;
    }
    
}
