package valencia;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Random;

public class TestMemoryPerformance {

    public static void main(String[] args) throws Exception {        
        int numIters = 3;
        int size = 16384;
        int total = 0;
        Random random = new Random(1);
        while(size > 0) {
            System.out.println(size);
            long start;
            double[] times = new double[numIters];
            int[] sindexes = new int[size];
            for(int i = 0; i < size; i++)
                sindexes[i] = i;
            int[] rindexes = new int[size];
            for(int i = 0; i < size; i++)
                rindexes[i] = random.nextInt(size);
            int[] array = new int[size];
            for(int i = 0; i < size; i++)
                array[i] = 0;
            System.gc();

            // int[] serial access, array
            for(int iter = 0; iter < numIters; iter++) {
                start = System.nanoTime();
                total = 0;
                for(int i = 0; i < size; i++)
                    total += array[sindexes[i]];
                times[iter] = System.nanoTime() - start;
            }
            printTimes("Serial int[]", times, size);

            // int[] random access
            for(int iter = 0; iter < numIters; iter++) {
                start = System.nanoTime();
                total = 0;
                for(int i = 0; i < size; i++)
                    total += array[rindexes[i]];
                times[iter] = System.nanoTime() - start;
            }
            printTimes("Random int[]", times, size);

            // byte buffers
            ByteBuffer direct = ByteBuffer.allocateDirect(4*size);
            ByteBuffer heap = ByteBuffer.allocate(4*size);
            File temp = File.createTempFile("test-mem", ".dat");
            temp.deleteOnExit();
            RandomAccessFile file = new RandomAccessFile(temp, "rw");
            byte[] writeBuffer = new byte[64*1024];
            for(int i = 0; i < 4*size; i += writeBuffer.length)
                file.write(writeBuffer);
            MappedByteBuffer mapped = file.getChannel().map(MapMode.READ_WRITE, 0, file.length());
            System.gc();
            ByteBuffer[] buffers = new ByteBuffer[] { heap, direct, mapped };
            String[] names = {"heap buffer", "direct buffer", "mapped buffer"};
            for(int d = 0; d < buffers.length; d++) {
                for(int i = 0; i < size; i++)
                    buffers[d].putInt(0);

                // byte buffer serial
                for(int iter = 0; iter < numIters; iter++) {
                    start = System.nanoTime();
                    total = 0;
                    for(int i = 0; i < size; i++)
                        total += buffers[d].getInt(sindexes[i] << 2);
                    times[iter] = System.nanoTime() - start;
                }
                printTimes("Serial " + names[d], times, size);

                // byte buffer random
                for(int iter = 0; iter < numIters; iter++) {
                    start = System.nanoTime();
                    total = 0;
                    for(int i = 0; i < size; i++)
                        total += buffers[d].getInt(rindexes[i] << 2);
                    times[iter] = System.nanoTime() - start;
                }
                printTimes("Random "+ names[d], times, size);
            }
            temp.delete();
            size *= 2;
        }
        System.exit(total);
    }
    
    private static void printTimes(String experiment, double[] times, int size) {
        NumberFormat format = NumberFormat.getInstance();
        format.setMaximumFractionDigits(2);
        Arrays.sort(times);
        System.out.println(experiment + " (ns/access): " + format.format(times[times.length/2]/size));
    }

}
