package cn.leo.mapred.test.flowcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountMap extends Mapper<LongWritable, Text, Text, FlowBean> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] words = line.split("\t");

		if (words.length < 1) {
			return;
		}

		FlowBean flowBean = new FlowBean();

		String cellphoneNo = words[0];

		Long upFlow = Long.valueOf(words[words.length - 3]);

		Long downFlow = Long.valueOf(words[words.length - 2]);

		flowBean.set(upFlow, downFlow, cellphoneNo);

		context.write(new Text(cellphoneNo), flowBean);

	}
}
