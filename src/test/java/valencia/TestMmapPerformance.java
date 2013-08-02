package valencia;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class TestMmapPerformance {

    public static void main(String[] args) throws Exception {
        boolean pretouch = Boolean.valueOf(args[2]);
        ByteBuffer[] buffers = map(args[0], pretouch);
        System.out.println("Mapped " + buffers.length + " buffers.");
        int requests = Integer.parseInt(args[1]);
        Random random = new Random();
        System.out.println("First result is " + random.nextInt());
        int total = 0;
        long start = System.nanoTime();
        for(int i = 0; i < requests; i++) {
            int bufferIndex = i % buffers.length;
            ByteBuffer buffer = buffers[bufferIndex];
            int offset = random.nextInt(buffer.limit());
            total += buffer.get(offset);
        }
        double ellapsedSecs = (System.nanoTime() - start)/(1000.0*1000.0*1000.0);
        System.out.println(requests / ellapsedSecs);
        System.exit(0 & total);
    }
    
    private static ByteBuffer[] map(String fileName, boolean preTouch) throws Exception {
        RandomAccessFile file = new RandomAccessFile(fileName, "rw");
        FileChannel channel = file.getChannel();
        List<MappedByteBuffer> buffers = new ArrayList<MappedByteBuffer>();
        long length = file.length();
        for(long mapped = 0; mapped < length;) {
            int size = (int) Math.min(length - mapped, Integer.MAX_VALUE);
            buffers.add(channel.map(MapMode.READ_WRITE, 0, size));
            mapped += size;
        }
        if(preTouch) {
            for(ByteBuffer buffer: buffers) {
                for(int i = 0; i < 10000 && buffer.hasRemaining(); i++) {
                    buffer.get();
                }
            }
        }
        return buffers.toArray(new ByteBuffer[buffers.size()]);
    }
    
}
