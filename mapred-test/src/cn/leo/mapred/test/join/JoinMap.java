package cn.leo.mapred.test.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JoinMap extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] words = line.split("\t");

		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		
		if (words.length == 2) {
			String changshangId = words[0];
			String changshangName = words[1] + "-" + fileName;
			context.write(new Text(changshangId), new Text(changshangName));
		}
		if (words.length == 3) {
			String changshangId = words[2];
			String chanpingName = words[1] + "-" + fileName;
			context.write(new Text(changshangId), new Text(chanpingName));
		}
	}
}
