package cn.leo.mapred.test.flowcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// cn.leo.mapred.test.flowcount.FlowCountRunner
public class FlowCountRunner {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowCountRunner.class);

		job.setMapperClass(FlowCountMap.class);
		job.setPartitionerClass(FlowCountPartitioner.class);
		job.setReducerClass(FlowCountReduce.class);
		job.setNumReduceTasks(10);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		// FileInputFormat.setInputPaths(job, args[0]);
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileInputFormat.setInputPaths(job, "D:/fc/HTTP_20130313143750.dat");
		FileOutputFormat.setOutputPath(job, new Path("D:/fc/out"));

		job.waitForCompletion(true);

	}
}
