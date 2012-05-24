package     com.spbsu.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: svasilinets
 * Date: 02.05.12
 * Time: 17:10
 */
public class CountryTimeKey implements WritableComparable<CountryTimeKey> {

    private String country;
    private long timeStamp;


    public CountryTimeKey(String country, long  timeStamp) {
        this.timeStamp = timeStamp;
        this.country = country;
    }

    public CountryTimeKey() {
    }

    public String getCountry() {
        return country;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    @Override
    public int compareTo(CountryTimeKey o) {
        if (o == this) {
            return 0;
        }

        if (country.equals(o.country)) {
            if (timeStamp == o.timeStamp) {
                return 0;
            }
            return timeStamp < o.timeStamp ? -1 : 1;
        }
        return country.compareTo(o.country);
    }


    @Override
    public int hashCode() {
        return 157 * country.hashCode() + ((int) (timeStamp ^ (timeStamp >>> 32)));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CountryTimeKey) {
            CountryTimeKey ck = (CountryTimeKey) obj;
            return ck.getCountry().equals(country) && ck.timeStamp == timeStamp;
        } else {
            return false;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(country);
        dataOutput.writeLong(timeStamp);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        country = input.readUTF();
        timeStamp = input.readLong();
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(CountryTimeKey.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static {                                        // register this comparator
        WritableComparator.define(CountryTimeKey.class, new Comparator());
    }

}
