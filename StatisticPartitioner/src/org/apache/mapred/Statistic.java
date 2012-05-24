package org.apache.mapred;

import org.apache.hadoop.io.Writable;

import java.util.Map;

/**
 * User: svasilinets
 * Date: 12.03.12
 * Time: 18:03
 */
public interface Statistic<K, V, M> extends Writable {

    void add(K key, V value);

    Converter<K, M> getConverter();
    
    Map<M, Integer> export(); 

}
