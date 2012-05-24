import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;

import java.io.DataInput;
import java.io.IOException;
import java.util.*;

/**
 * User: svasilinets
 * Date: 25.03.12
 * Time: 4:41
 */
public class Compare {

    private static final String WORKING_DIR = "statistic";

    private static final String COMPARISON_DIR = "comparison";


    private static final Set<String> except = new HashSet<String>(){{
        add("predicted");
    }};


    private static void showStatisticForJob(FileSystem fs, String jobName, String jobId) throws IOException {

        Path p = new Path(WORKING_DIR, jobName);
        Path compDir = new Path(p, new Path(COMPARISON_DIR, jobId));
        FileStatus[] statuses = fs.listStatus(compDir);
        LightMTreeMap<String, Path> treeMap = new LightMTreeMap<String, Path>();
        for (FileStatus status : statuses) {

            String name = status.getPath().getName();
            int ind = name.indexOf("_");

            if (ind == -1) {
                continue;
            }

            treeMap.put(name.substring(0, ind), status.getPath());
        }

        for (String key : treeMap.keySet()) {

            Iterable<Path> paths = treeMap.getAllByKey(key);
            if (except.contains(key) && paths.iterator().hasNext()){
                final Path e = paths.iterator().next();
                paths = new LinkedList<Path>(){{add(e);}};
            }

            int[] stats = Util.countFor(fs, paths);
            System.out.println(key + ": " + Arrays.toString(stats));
        }

    }




    public static void main(String[] args) throws IOException {


        Configuration conf = new Configuration();

        if (args.length != 1) {
            System.out.println("compare <jobID>");
            return;
        }
        FileSystem fs = FileSystem.get(conf);
        showStatisticForJob(fs,  Util.getJobName(args[0]), args[0]);
    }
}
