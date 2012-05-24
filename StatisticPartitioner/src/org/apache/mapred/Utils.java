package org.apache.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataOutput;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

/**
 * User: svasilinets
 * Date: 25.03.12
 * Time: 3:26
 */
class Utils {

    private Utils() {
    }

    static String name;


    private static String initName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            Random r = new Random();
            int k = r.nextInt(10000);
            e.printStackTrace();
            return Integer.toString(k);
        }
    }

    /**
     * @return Machine name,  if some error occurs it will return some random id for machine.
     *         This id isn't stable (For different jobs it will be differrent);
     */
    static String getMachineName() {
        if (name == null) {
            name = initName();
        }
        return name;
    }


    static void writeIntArray(DataOutput dataOutput, int[] array) throws IOException {
        dataOutput.writeInt(array.length);
        for (int i: array){
            dataOutput.writeInt(i);
        }

    }

    
    static void writeIntArray(Path path, int[] array) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataOutputStream out =  fs.create(path);
        Utils.writeIntArray(out, array);
        out.close();
    }

}
