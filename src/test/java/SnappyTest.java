import org.xerial.snappy.SnappyInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class SnappyTest {
    public static void main(String[] args) {

        File file = new File("/Users/wangleigis163.com/Downloads/test.snappy"); //待解压文件
        File out = new File("/Users/wangleigis163.com/Downloads/test-out");  //解压后文件

        byte[] buffer = new byte[1024 * 1024 * 8];
        FileInputStream fi = null;
        FileOutputStream fo = null;
        SnappyInputStream sin = null;
        try
        {
            fo = new FileOutputStream(out);
            fi = new FileInputStream(file.getPath());
            sin = new SnappyInputStream(fi);

            while(true)
            {
                int count = sin.read(buffer, 0, buffer.length);
                if(count == -1) { break; }
                fo.write(buffer, 0, count);
            }
            fo.flush();
        }
        catch(Throwable ex)
        {
            ex.printStackTrace();
        }
        finally
        {
            if(sin != null) { try { sin.close(); } catch(Exception x) {} }
            if(fi != null) { try { fi.close(); } catch(Exception x) {} }
            if(fo != null) { try { fo.close(); } catch(Exception x) {} }
        }

    }
}
