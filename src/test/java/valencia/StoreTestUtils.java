package valencia;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.io.*;
import com.google.common.io.*;

public class StoreTestUtils {

    private static Random random = new Random(1);

    public static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    public static Record randomRecord() {
        return new Record(randomBytes(5), randomBytes(10));
    }

    public static List<Record> randomRecords(int count) {
        List<Record> records = new ArrayList<Record>();
        for(int i = 0; i < count; i++)
            records.add(randomRecord());
        return records;
    }

    public static LogSegment createEmptyLogSegment(int maxSize) throws IOException {
        File dir = Files.createTempDir();
        File file = new File(dir, "0.log");
        LogSegment segment = LogSegment.create(file, maxSize, maxSize);
        file.deleteOnExit();
        return segment;
    }

}