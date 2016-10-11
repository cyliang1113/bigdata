package cn.leo.mapred.test.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMap extends Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] words = line.split(" ");

		for (String string : words) {
			context.write(new Text(string), new LongWritable(1));
		}

	}
}
