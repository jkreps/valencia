package valencia;

public final class LogEntry {

    private final long offset;
    private final Record record;

    public LogEntry(long offset, Record record) {
        this.offset = offset;
        this.record = record;
    }

    public long offset() {
        return offset;
    }

    public Record record() {
        return record;
    }
}
