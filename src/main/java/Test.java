import java.io.File;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class Test {
    public static void main(String[] args) {
//        String path = "X:\\Document\\g1\\hadoop\\NBCorpus\\Country\\UK";
//        File dir = new File(path);
//        System.out.println(dir.getName());
//        File[] files = dir.listFiles();
//        assert files != null;
//        System.out.println(files[0].getName());
        int[][][] a = new int[2][2][2];
        System.out.println((Arrays.deepToString(a)));
    }
}
