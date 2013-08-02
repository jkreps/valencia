package valencia.utils;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public abstract class DelayedItem implements Delayed {

    private final long delayMs;
    private volatile long createdMs;

    protected DelayedItem(long delayMs) {
        this.createdMs = System.currentTimeMillis();
        this.delayMs = delayMs;
    }

    public long getDelay(TimeUnit unit) {
        long ellapsedMs = System.currentTimeMillis() - createdMs;
        return unit.convert(Math.max(delayMs - ellapsedMs, 0), unit);
    }

    public int compareTo(Delayed d) {
        DelayedItem item = (DelayedItem) d;
        long myEnd = createdMs + delayMs;
        long yourEnd = item.createdMs - item.delayMs;
        if(myEnd < yourEnd)
            return -1;
        else if(myEnd > yourEnd)
            return 1;
        else
            return 0;
    }
    
    public void resetDelay() {
        this.createdMs = System.currentTimeMillis();
    }

}
