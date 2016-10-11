package cn.leo.mapred.test.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

		long count = 0;
		
		for (LongWritable longWritable : values) {
			count += longWritable.get();
		}
		context.write(key, new LongWritable(count));

	}
}
