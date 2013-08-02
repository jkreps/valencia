package valencia;


public class LogCorruptionException extends RuntimeException {
    
    private static final long serialVersionUID = 1;

    public LogCorruptionException(String message) {
        super(message);
    }
    
    public LogCorruptionException() {
        super();
    }
}
