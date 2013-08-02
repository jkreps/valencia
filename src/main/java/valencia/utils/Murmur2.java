package valencia.utils;

/**
 * murmur hash 2.0.
 * 
 * This is a re-implementation of the original C code plus some additional
 * features.
 * 
 * Modified version of public domain domain code by Viliam Holub
 * 
 */
public final class Murmur2 {

    // all methods static; private constructor.
    private Murmur2() {}

    /**
     * Generates 32 bit hash from byte array of the given length and seed.
     * 
     * @param data byte array to hash
     * @param length length of the array to hash
     * @param seed initial seed value
     * @return 32 bit hash of the given array
     */
    public static int hash32(final byte[] data, int length, int seed) {
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for(int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8)
                    + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch(length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= (data[length & ~3] & 0xff);
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    /**
     * Generates 32 bit hash from byte array with default seed value.
     * 
     * @param data byte array to hash
     * @param length length of the array to hash
     * @return 32 bit hash of the given array
     */
    public static int hash32(final byte[] data, int length) {
        return hash32(data, length, 0x9747b28c);
    }
}
