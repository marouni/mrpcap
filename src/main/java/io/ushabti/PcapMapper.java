package io.ushabti;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;

public class PcapMapper extends Mapper<BytesWritable, BytesWritable, Text, IntWritable> {
	
 
 private static final String DBNAME="GeoLite2-City.mmdb";
 
 private Text word = new Text();
 private final static IntWritable one = new IntWritable(1);
 private byte[] binaryValue;
 private byte[] binaryIpAddress = new byte[4];
 private InputStream is;
 private DatabaseReader dbReader;
 private CityResponse dbResponse;
  
 @Override
protected void setup(Context context) throws IOException, InterruptedException {
	 Configuration conf = context.getConfiguration();
	 Path [] files = context.getLocalCacheFiles();
	 
	 for(Path file : files) {
		 if(file.getName().equals(DBNAME));
		 FileSystem fs = FileSystem.getLocal(conf);
		 is = fs.open(file);
	 }
	
	 dbReader = new DatabaseReader.Builder(is).build();
}

@Override
 public void map(BytesWritable key, BytesWritable value, Context contex) throws IOException, InterruptedException {
	 
	 // Get Bytes
	 binaryValue = value.getBytes();
	 
	 // Extract IP address
	 binaryIpAddress[0] = binaryValue[27];
	 binaryIpAddress[1] = binaryValue[28];
	 binaryIpAddress[2] = binaryValue[29];
	 binaryIpAddress[3] = binaryValue[30];
	 
	 // Find GEO data of the IP address
	 try {
		dbResponse = dbReader.city(InetAddress.getByAddress(binaryIpAddress));
		word.set(dbResponse.getCountry().getName()+"-"+dbResponse.getCity().getName());
		contex.write(word, one);
	} catch (GeoIp2Exception e) {
		e.printStackTrace();
	}
 }
}