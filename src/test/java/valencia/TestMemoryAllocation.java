package valencia;

import java.nio.ByteBuffer;

public class TestMemoryAllocation {

    public static void main(String[] args) {
        int segments = Integer.parseInt(args[0]);
        int size = Integer.parseInt(args[1]);
        ByteBuffer[] buffers = new ByteBuffer[segments];
        for(int i = 0; i < buffers.length; i++) {
            System.out.println("Allocating buffer " + i);
            ByteBuffer buffer = ByteBuffer.allocateDirect(size);
            for(int j = 0; j < size / 8; j++)
                buffer.putLong(0);
            buffers[i] = buffer;
        }
    }
    
}
