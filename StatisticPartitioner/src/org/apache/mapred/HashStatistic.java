package org.apache.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * User: svasilinets
 * Date: 12.03.12
 * Time: 18:08
 */
public class HashStatistic<K, V> implements Statistic<K, V, Integer> {

    private Log LOG = LogFactory.getLog(HashStatistic.class);

    private final static int DEFAULT_CAPACITY = 10000;

    private static class HashConverter<K> implements  Converter<K, Integer>{

        private final int n;

        private HashConverter(int n) {
            this.n = n;
        }

        @Override
        public Integer convert(K key) {
            return (n + (key.hashCode() % n)) % n;
        }
    }

    private int capacity;

    private final Converter<K, Integer> converter;

    private int[] hashs;


    public HashStatistic() {
        capacity = DEFAULT_CAPACITY;
        hashs = new int[capacity];
        converter = new HashConverter<K>(capacity);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Utils.writeIntArray(dataOutput, hashs);
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int savedCap = dataInput.readInt();
        if (savedCap != capacity){
            LOG.error("data input has different capacity cant add!! read " + savedCap + " " + capacity);
            return;
        }
        for (int i = 0; i < capacity; i++){
            hashs[i] += dataInput.readInt();
        }

        LOG.info("check " + Arrays.toString(hashs));
    }

    @Override
    public void add(K key, V value) {
        //make it non-negative

        int h = converter.convert(key);
        synchronized (hashs) {
            hashs[h]++;
        }
    }

    @Override
    public Converter<K, Integer> getConverter() {
        return converter;
    }

    @Override
    public Map<Integer, Integer> export() {
        
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        for (int i = 0; i < capacity; i++){
            if (hashs[i] != 0){
                map.put(i, hashs[i]);
            }
        }
        return map;
    }
}
