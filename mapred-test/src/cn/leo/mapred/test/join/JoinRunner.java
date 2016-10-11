package cn.leo.mapred.test.join;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//cn.leo.mapred.test.join.JoinRunner
public class JoinRunner {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(JoinRunner.class);

		job.setMapperClass(JoinMap.class);
		job.setReducerClass(JoinReduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// FileInputFormat.setInputPaths(job, args[0]);
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileInputFormat.setInputPaths(job, "E:/hadoop/join/");
		FileOutputFormat.setOutputPath(job, new Path("E:/hadoop/join/out"));

		job.waitForCompletion(true);

	}
}
