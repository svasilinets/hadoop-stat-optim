import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.LinkedList;

/**
 * User: svasilinets
 * Date: 27.03.12
 * Time: 10:50
 */
public class CompareKeys {


    private static final String WORKING_DIR = "statistic";


    private final static String STATS_DIR = "stats";


    private static int[] getStats(String jobId, String jobName) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        Path p = new Path(WORKING_DIR, jobName);
        Path compDir = new Path(p, STATS_DIR);

        LinkedList<Path> list = new LinkedList<Path>();
        for (FileStatus status : fs.listStatus(compDir)) {

            Path path = status.getPath();
            if (path.getName().contains(jobId)){
                list.add(path);
            }
        }


        return Util.countFor(fs, list);

    }

    
    private static void compare(int[] a, int[] b, int diff){
        for (int i = 0; i < a.length; i++){
            int di = Math.abs(a[i] - b[i]);
            if (di > diff){
                System.out.println(i + " diff: " + di);
            }
        }
    }

    public static void main(String[] args) throws IOException {
     
        if (args.length != 3) {
            System.out.println("compare <jobID1> <JobID2> <diff>");
            return;
        }
     
        String jobName1 = Util.getJobName(args[0]);
        String jobName2 = Util.getJobName(args[1]);
        if (!jobName1.equals(jobName2)){
            System.out.println("Cant compare different jobs");
            return;
        }


        compare(getStats(args[0], jobName1), getStats(args[1], jobName2), Integer.parseInt(args[2]));

    }
}
