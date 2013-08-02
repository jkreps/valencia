package valencia;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;


public class TestHashUniformity {
    
    private static HashFunction hashFun = Hashing.murmur3_32();

    public static void main(String[] args) {
        int numValues = Integer.parseInt(args[0]);
        int[] values = new int[numValues];
        ByteBuffer buffer = ByteBuffer.allocate(4);
        for(int i = 0; i < numValues; i++) {
            buffer.putInt(i);
            buffer.rewind();
            values[i] = hash(buffer);
            if(i % 10000000 == 0)
                System.out.println(i + " hashes computed.");
        }
        Arrays.sort(values);
        int collisions = 0;
        int prev = values[0];
        for(int i = 1; i < numValues; i++) {
            int curr = values[i];
            if(curr == prev)
                collisions++;
            prev = curr;
        }
        System.out.println("Collisions = " + collisions + ", rate: " + collisions / (double) numValues);
    }
    
    public static int hash(ByteBuffer bytes) {
        int limit = bytes.limit();
        Hasher hasher = hashFun.newHasher();
        for(int i = 0; i < limit; i++)
            hasher.putByte(bytes.get(i));
        return Math.abs(hasher.hash().asInt());
    }
    
}
