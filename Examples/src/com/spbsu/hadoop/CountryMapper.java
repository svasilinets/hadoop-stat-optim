package com.spbsu.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;

/**
 * User: svasilinets
 * Date: 02.05.12
 * Time: 16:34
 */
class CountryMapper extends Mapper<Object, Text, CountryTimeKey, LogRecord> {

    private IpCountriesData db = null;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (db == null) {
            System.out.println("failed to load db");
            return;
        }
        try {
            LogRecord record = LogRecord.parseLogString(value.toString());
            String country = db.getCountryByIp(record.getIp());
            context.write(new CountryTimeKey(country, record.getTimestamp()), record);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        loadCached(context.getConfiguration());
    }


    void loadCached(Configuration conf) {
        try {
            String countryDBCacheName = new Path(SessionDuration.HDFS_COUNTRIES_IP_CSV).getName();
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
            if (null != cacheFiles && cacheFiles.length > 0) {
                for (Path cachePath : cacheFiles) {
                    if (cachePath.getName().equals(countryDBCacheName)) {
                        db = new IpCountriesData(cachePath.toString());
                        break;
                    }
                }
            }
        } catch (IOException ioe) {
            System.err.println("IOException reading from distributed cache");
            System.err.println(ioe.toString());
        }
    }
}