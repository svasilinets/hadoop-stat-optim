package org.apache.mapred;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * User: svasilinets
 * Date: 02.05.12
 * Time: 16:55
 */
public class SecondaryPartitioner<KEY, PK, VALUE> extends Partitioner<KEY, VALUE> implements Configurable {

//
//    private Partitioner<PK, VALUE> partitioner;
//    private KeyDecomposer<KEY, PK> de
//
//
//    public static final  String REAL_PARTITIONER_KEY = "mapred.job.real.partitioner";
//
//    @Override
//    public int getPartition(KEY key, VALUE value, int i) {
//        return partitioner.getPartition();
//    }
//
//    @Override
//    public void setConf(Configuration entries) {
//
//        String partClassName =  entries.get(REAL_PARTITIONER_KEY, HashPartitioner.class.getName());
//        try{
//            partitioner = (Partitioner<PK, VALUE>) entries.getClassByName(partClassName).newInstance();
//        }catch (Exception e){
//            e.printStackTrace();
//            partitioner = new HashPartitioner<PK, VALUE>();
//        }
//
//    }
//
//    @Override
//    public Configuration getConf() {
//        return null;
//    }

    @Override
    public void setConf(Configuration entries) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }

    @Override
    public int getPartition(KEY key, VALUE value, int i) {
        return 0;
    }
}
