import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * User: svasilinets
 * Date: 05.04.12
 * Time: 12:41
 */
public class KeysOnReducers {


    private static final String WORKING_DIR = "statistic";

    private static final String COMPARISON_DIR = "comparison";

    
    public static void showKeysOnReducers(FileSystem fs, String jobId) throws IOException {

        Path p = new Path(WORKING_DIR, Util.getJobName(jobId));
        Path compDir = new Path(p, new Path(COMPARISON_DIR, jobId));
        Path kF = new Path(compDir, "a");
        for (FileStatus status: fs.listStatus(kF)){
            FSDataInputStream in = fs.open(status.getPath());
            System.out.println(in.readInt());
            in.close();
        }

    }
    
    
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        if (args.length != 1) {
            System.out.println("red <jobID>");
            return;
        }
        FileSystem fs = FileSystem.get(conf);
        showKeysOnReducers(fs, args[0]);
    }
}
