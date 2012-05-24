package com.spbsu.hadoop;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.mapred.HStatisticalPartitioner;

import java.io.IOException;

/**
 * User: svasilinets
 * Date: 28.04.12
 * Time: 13:41
 */
public class SessionDuration {


    public static final String HDFS_COUNTRIES_IP_CSV = "/data/countries.csv";


    public static class CountryGroupingComparator
            implements RawComparator<CountryTimeKey> {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

            return WritableComparator.compareBytes(b1, s1, l1 - Long.SIZE / 8,
                    b2, s2, l2 - Long.SIZE / 8);
        }

        @Override
        public int compare(CountryTimeKey o1, CountryTimeKey o2) {
            String l = o1.getCountry();
            String r = o2.getCountry();
            return l.compareTo(r);
        }
    }


    public static class HSPartitioner extends Partitioner<CountryTimeKey, LogRecord> implements Configurable {

        private HStatisticalPartitioner<String, LogRecord> partitioner = new HStatisticalPartitioner<String, LogRecord>();


        @Override
        public void setConf(Configuration entries) {
            partitioner.setConf(entries);
        }

        @Override
        public Configuration getConf() {
            return null;
        }

        @Override
        public int getPartition(CountryTimeKey countryTimeKey, LogRecord logRecord, int i) {
            return partitioner.getPartition(countryTimeKey.getCountry(), logRecord, i);
        }
    }

    public static class DPartitioner extends Partitioner<CountryTimeKey, LogRecord> {

        private Partitioner<String, LogRecord> partitioner = new HashPartitioner<String, LogRecord>();

        @Override
        public int getPartition(CountryTimeKey countryTimeKey, LogRecord logRecord, int i) {
            return partitioner.getPartition(countryTimeKey.getCountry(), logRecord, i);
        }
    }


    static void cacheCountriesIpCSV(Configuration conf, String localPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path destPath = new Path(HDFS_COUNTRIES_IP_CSV);
        fs.copyFromLocalFile(false, new Path(localPath), destPath);
        DistributedCache.addCacheFile(destPath.toUri(), conf);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "session_duration");

        job.setJarByClass(SessionDuration.class);
        job.setMapperClass(CountryMapper.class);
        job.setReducerClass(CountryReducer.class);
        job.setMapOutputKeyClass(CountryTimeKey.class);
        job.setMapOutputValueClass(LogRecord.class);

        job.setGroupingComparatorClass(CountryGroupingComparator.class);
        if (args.length > 3 && args[3].equals("def")) {
            job.setPartitionerClass(DPartitioner.class);
        } else {
            job.setPartitionerClass(HSPartitioner.class);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        cacheCountriesIpCSV(job.getConfiguration(), args[0]);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
