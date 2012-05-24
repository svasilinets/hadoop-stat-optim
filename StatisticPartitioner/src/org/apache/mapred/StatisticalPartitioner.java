package org.apache.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;
import java.util.*;

/**
 * User: svasilinets
 * Date: 01.03.12
 * Time: 18:15
 */
public abstract class StatisticalPartitioner<KEY, VALUE, M> extends Partitioner<KEY, VALUE> implements Configurable {

    private static final long SLEEP_PERIOD = 500L;

    private static final Log LOG = LogFactory.getLog(StatisticalPartitioner.class.getName());

    // gc doesn't collect us to early. Because here is public static reference to our partitioner
    public static List<StatisticalPartitioner<?, ?, ?>> longLivePartitioner =
            new LinkedList<StatisticalPartitioner<?, ?, ?>>();


    private static final String WORKING_DIRECTORY = "statistic";
    private static final String COMPARISON_FOLDER = "comparison";
    private static final String STATISTIC_FOLDER = "stats";


    private static final String JOB_NAME_KEY = "mapred.job.name";
    private static final String JOB_ID_KEY = "mapred.job.id";
    private static final String TASK_ID_KEY = "mapred.task.id";

    private static Configuration statConfig = new Configuration();

    static {
        statConfig.set("statistic", "on");
    }

    private Thread checkUsageThread;

    private volatile boolean usedOnce = false;

    private volatile boolean usedSinceLastTime = false;

    private Runnable checkRunnable = new Runnable() {
        @Override
        public void run() {
            try {
                LOG.info("check thread was started");
                while (!usedOnce || usedSinceLastTime) {

                    LOG.error("check usage");
                    usedSinceLastTime = false;

                    try {
                        Thread.sleep(SLEEP_PERIOD);
                    } catch (InterruptedException e) {
                        LOG.error("check shit happend");
                        e.printStackTrace();
                    }
                }
                LOG.info("statistical partitioner finished it's work");
                onStop();
                longLivePartitioner.remove(StatisticalPartitioner.this);
            } finally {
                LOG.info("check end");
            }
        }


    };

    private Statistic<KEY, VALUE, M> newStatistic;

    private Statistic<KEY, VALUE, M> oldStatistic;

    private Converter<KEY, M> converter;

    private Map<M, Integer> partition;


    private Configuration conf;

    private String jobName;
    private String jobId;
    private String taskId;

    private StandardPartition compare;
    private int[] resultPartition;


    protected Map<M, Integer> getPartition(int k) {
        Map<M, Integer> st = oldStatistic.export();
        LightMTreeMap<Integer, M> map = new LightMTreeMap<Integer, M>();
        for (Map.Entry<M, Integer> entry : st.entrySet()) {
            map.put(entry.getValue(), entry.getKey());
        }

        LightMTreeMap<Integer, Integer> sumToBins = new LightMTreeMap<Integer, Integer>();

        HashMap<M, Integer> numToBins = new HashMap<M, Integer>();

        for (int i = 0; i < k; i++) {
            sumToBins.put(0, i);
        }
        for (int value : map.descendingKeySet()) {
            for (M key : map.getAllByKey(value)) {
                int sum = sumToBins.firstKey();
                int binNum = sumToBins.remove(sum);
                numToBins.put(key, binNum);
                int s = sum + value;
                sumToBins.put(s, binNum);
            }
        }
        return numToBins;
    }


    private void compare(int k) {
        int[] oldPartitioner = new int[k];
        Map<M, Integer> st = oldStatistic.export();
        for (M key : st.keySet()) {
            oldPartitioner[(key.hashCode() & Integer.MAX_VALUE) % k] += st.get(key);
        }
        LOG.info("comparison old: " + Arrays.toString(oldPartitioner));
        int[] newPart = new int[k];
        Map<M, Integer> s = getPartition(k);
        for (M key : s.keySet()) {
            newPart[s.get(key)] += st.get(key);
        }
        LOG.info("comparison new: " + Arrays.toString(newPart));

    }


    void onFirstKey(int numReduceTasks) {
        if (oldStatistic != null) {
            partition = getPartition(numReduceTasks);
        }
        resultPartition = new int[numReduceTasks];
        compare = new StandardPartition(3, 4, numReduceTasks);


    }


    void onStop() {
        writeStatistic(newStatistic);
        writeComparison();
    }

    @Override
    public int getPartition(KEY key, VALUE value, int numReduceTasks) {

        usedSinceLastTime = true;
        if (!usedOnce) {
            onFirstKey(numReduceTasks);
        }

        compare.update(key);
        usedOnce = true;
        newStatistic.add(key, value);

        M converted = converter != null ? converter.convert(key) : null;
        int reducerNum = partition != null && partition.containsKey(converted) ? partition.get(converted) :
                (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        resultPartition[reducerNum]++;
        return reducerNum;
    }


    @Override
    public void setConf(Configuration entries) {
        conf = entries;
        jobName = entries.get(JOB_NAME_KEY);
        jobId = entries.get(JOB_ID_KEY);
        taskId = entries.get(TASK_ID_KEY);
        LOG.info("SetConf was called! " + longLivePartitioner.size());


        for (Map.Entry<String, String> ent : entries) {
            LOG.info("entry " + ent.getKey() + " " + ent.getValue());
        }
        longLivePartitioner.add(this);
        checkUsageThread = new Thread(checkRunnable);
        checkUsageThread.setPriority(Thread.MAX_PRIORITY);
        checkUsageThread.start();

        oldStatistic = readStatistic();
        newStatistic = newStatistic();
        if (oldStatistic != null) {
            converter = oldStatistic.getConverter();
        }

    }

    private Path getStatsPath() {
        Path p = new Path(WORKING_DIRECTORY, jobName);
        return new Path(p, STATISTIC_FOLDER);
    }

    private Path getComparisonPath() {
        Path p = new Path(WORKING_DIRECTORY, jobName);
        return new Path(p, COMPARISON_FOLDER);
    }


    private Statistic<KEY, VALUE, M> readStatistic() {
        try {
            Statistic<KEY, VALUE, M> st = newStatistic();
            FileSystem fs = FileSystem.get(statConfig);
            Configuration c = new Configuration();
            c.setBoolean("statistic", true);
            Path stFolder = getStatsPath();
            if (fs.exists(stFolder)) {
                LOG.info("statistic exists");
                FileStatus[] list = fs.listStatus(stFolder);
                if (list == null || list.length == 0) {
                    LOG.info("statistic file list is empty");
                    return null;
                }

                for (FileStatus f : list) {
                    if (f.getPath().toString().contains(jobId)) {
                        continue;
                    }
                    LOG.info("reading statistic from " + f.getPath());
                    FSDataInputStream in = null;
                    try {
                        in = fs.open(f.getPath());
                        st.readFields(in);
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    } finally {
                        if (in != null) {
                            in.close();
                        }
                    }
                }
                return st;
            } else {
                LOG.info("statistic folder doesn't exists");
            }


        } catch (IOException e) {
            e.printStackTrace();
        }


        return null;
    }

    private void writeStatistic(Statistic<KEY, VALUE, M> st) {
        LOG.info("writing statistic to the disk");
        try {
            String name = Utils.getMachineName() + taskId;
            LOG.info("got machine name = " + name);
            FileSystem fs = FileSystem.get(statConfig);
            Path file = new Path(getStatsPath(), jobId + "_" + name);
            FSDataOutputStream outputStream = fs.create(file);
            st.write(outputStream);
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    private int[] getPartitionByStats(Statistic<KEY, VALUE, M> statistic) {

        Map<M, Integer> st = statistic.export();
        int[] newPart = new int[resultPartition.length];
        for (M key : partition.keySet()) {
            newPart[partition.get(key)] += st.get(key);
        }
        return newPart;
    }


    private void writeComparison() {
        try {
            FileSystem fs = FileSystem.get(statConfig);
            Path compPath = new Path(getComparisonPath(), jobId);

            String mname = Utils.getMachineName() + taskId;
            String namePattern = compPath + "/%d_" + mname;

            compare.writePartitions(fs, namePattern);
            Utils.writeIntArray(new Path(compPath, "result_" + mname), resultPartition);

            if (partition != null) {
                int[] predicted = getPartitionByStats(oldStatistic);
                Utils.writeIntArray(new Path(compPath, "predicted_" + mname), predicted);
            }
            LOG.info("result partition: " + Arrays.toString(resultPartition));

        } catch (IOException e) {
            e.printStackTrace();
        }


    }


        protected abstract Statistic<KEY, VALUE, M> newStatistic();


    @Override
    public Configuration getConf() {
        return null;
    }


}

class StandardPartition {

    private int[][] standard;

    StandardPartition(int... partitions) {
        standard = new int[partitions.length][];
        for (int i = 0; i < partitions.length; i++) {
            standard[i] = new int[partitions[i]];
        }
    }

    void update(Object key) {
        for (int i = 0; i < standard.length; i++) {
            int k = standard[i].length;
            standard[i][(key.hashCode() & Integer.MAX_VALUE) % k]++;
        }
    }


    void writePartitions(FileSystem fs, String namePattern) throws IOException {
        for (int i = 0; i < standard.length; i++) {
            Path p = new Path(String.format(namePattern, standard[i].length));
            Utils.writeIntArray(p, standard[i]);
        }

    }

}
