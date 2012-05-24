package org.apache.mapred;

/**
 * User: svasilinets
 * Date: 13.03.12
 * Time: 15:09
 */
public interface Converter<K, M> {
    M convert(K key);
}
