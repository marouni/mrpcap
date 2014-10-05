package io.ushabti;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf(), "PcapProcessing");
		job.setJarByClass(getClass());
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setMapperClass(PcapMapper.class);
		job.setReducerClass(PcapReducer.class);
		
		job.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.addCacheFile(new Path("GeoLite2-City.mmdb").toUri());
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(),new Driver(), args);
				System.exit(result);

	}

}
