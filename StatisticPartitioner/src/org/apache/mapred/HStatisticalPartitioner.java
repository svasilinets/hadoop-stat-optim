package org.apache.mapred;

/**
 * User: svasilinets
 * Date: 13.03.12
 * Time: 17:12
 */
public class HStatisticalPartitioner<KEY, VALUE> extends StatisticalPartitioner<KEY, VALUE, Integer>{
    @Override
    protected Statistic<KEY, VALUE, java.lang.Integer> newStatistic() {
        return new HashStatistic<KEY, VALUE>();
    }
}
