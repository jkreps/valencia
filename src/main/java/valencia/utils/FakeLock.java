package valencia.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A low-overhead lock that does no real locking and can act as a stand-in in cases where
 * there should be only a single thread.
 * 
 */
public class FakeLock extends ReentrantLock {
    
    private static final long serialVersionUID = 1L;
    
    private final AtomicReference<Thread> owner = new AtomicReference<Thread>();

    @Override
    public int getHoldCount() {
        return owner.get() == null? 0 : 1;
    }

    @Override
    protected Thread getOwner() {
        return owner.get();
    }

    @Override
    protected Collection<Thread> getQueuedThreads() {
        Thread t = owner.get();
        if(t == null)
            return Collections.emptyList();
        else
            return Collections.singleton(t);
    }

    @Override
    public int getWaitQueueLength(Condition condition) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Collection<Thread> getWaitingThreads(Condition condition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasWaiters(Condition condition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return getOwner() == Thread.currentThread();
    }

    @Override
    public boolean isLocked() {
        return getOwner() != null;
    }

    @Override
    public void lock() {
        Thread self = Thread.currentThread();
        Thread prev = this.owner.getAndSet(self);
        if(prev != null && prev != self)
            throw new IllegalStateException("Multithreaded access detected in single-threaded mode.");
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock();
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock() {
        lock();
        return true;
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        lock();
        return true;
    }

    @Override
    public void unlock() {
        boolean success = this.owner.compareAndSet(Thread.currentThread(), null);
        if(!success)
            throw new IllegalStateException("Multithreaded access detected in single-threaded mode.");
    }

}
