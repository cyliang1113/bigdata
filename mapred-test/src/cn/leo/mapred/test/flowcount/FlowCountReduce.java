package cn.leo.mapred.test.flowcount;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowCountReduce extends Reducer<Text, FlowBean, Text, FlowBean> {
	protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
		FlowBean rs = new FlowBean();

		Long sumUp = 0L;
		Long sumDown = 0L;

		for (FlowBean flowBean : values) {
			sumUp += flowBean.getUpFlow();
			sumDown += flowBean.getDownFlow();
		}

		rs.set(sumUp, sumDown, key.toString());

		context.write(key, rs);
	}
}
