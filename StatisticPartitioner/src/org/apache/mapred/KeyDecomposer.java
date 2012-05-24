package org.apache.mapred;

/**
 * User: svasilinets
 * Date: 02.05.12
 * Time: 15:03
 */
public interface KeyDecomposer<KEY, PK> {

   PK getPrimaryKey(KEY key);

}
