import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

/**
 * User: svasilinets
 * Date: 27.03.12
 * Time: 11:08
 */
public class Util {

    private Util() {
    }


    static String getJobName(String jobId) throws IOException {
        JobConf jobConf = new JobConf(new Configuration());
        JobClient client = new JobClient(jobConf);
        for (JobStatus jb : client.getAllJobs()) {
            if (jb.getJobID().toString().equals(jobId)) {
                return client.getJob(jb.getJobID()).getJobName();
            }
        }
        return "";
    }


    static int[] countFor(FileSystem fs, Iterable<Path> paths) throws IOException {

        int[] result = null;
        for (Path path : paths) {
            FSDataInputStream is = fs.open(path);
            try {
                int[] t = Util.readArray(is);

                if (result == null) {
                    result = new int[t.length];
                }
                for (int j = 0; j < result.length; j++) {
                    result[j] += t[j];
                }
            } catch (Exception e) {
                System.out.println("error at " + path);
            }
            is.close();

        }
        return result;
    }


    static int[] readArray(DataInput input) throws IOException {
        int length = input.readInt();
        int[] result = new int[length];
        for (int i = 0; i < length; i++) {
            result[i] = input.readInt();
        }
        return result;
    }


}
