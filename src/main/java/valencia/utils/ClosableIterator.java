package valencia.utils;

import java.util.Iterator;


public interface ClosableIterator<T> extends Iterator<T> {

    public void close();
    
}
