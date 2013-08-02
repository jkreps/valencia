package valencia;


public class ChecksumFailureException extends LogCorruptionException {
    
    private static final long serialVersionUID = 1;

    public ChecksumFailureException(String message) {
        super(message);
    }
    
    public ChecksumFailureException() {
        super();
    }

}
