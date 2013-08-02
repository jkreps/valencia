package valencia.utils;


import java.io.File;

public class Utils {

    public static void delete(File file) {
        if(file.isFile()) {
            file.delete();
        } else if(file.isDirectory()) {
            for(File f: file.listFiles())
                f.delete();
            file.delete();
        }
    }
    
}
