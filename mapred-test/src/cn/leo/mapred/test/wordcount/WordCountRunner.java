package cn.leo.mapred.test.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.leo.mapred.test.flowcount.FlowCountMap;

public class WordCountRunner {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(WordCountRunner.class);

		job.setMapperClass(FlowCountMap.class);
		job.setReducerClass(WordCountReduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, "hdfs://hadoop05:9000/wc/heihei.txt");
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop05:9000/wc/out"));

		// FileInputFormat.setInputPaths(job, "D:/wc/heihei");
		// FileOutputFormat.setOutputPath(job, new Path("D:/wc/out"));

		job.waitForCompletion(true);

	}
}
