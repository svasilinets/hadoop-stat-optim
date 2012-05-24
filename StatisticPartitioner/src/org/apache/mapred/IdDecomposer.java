package org.apache.mapred;

/**
 * User: svasilinets
 * Date: 02.05.12
 * Time: 16:46
 */
public class IdDecomposer implements KeyDecomposer {
    @Override
    public Object getPrimaryKey(Object o) {
        return o;
    }
}
