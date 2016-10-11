package cn.leo.mapred.test.flowcount;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowCountPartitioner extends Partitioner<Text, FlowBean> {

	private static HashMap<String, Integer> areaMap = new HashMap<>();

	static {

		areaMap.put("136", 0);
		areaMap.put("137", 1);
		areaMap.put("138", 2);
		areaMap.put("139", 3);

	}

	@Override
	public int getPartition(Text key, FlowBean value, int arg2) {
		String no = key.toString();
		Integer rs = areaMap.get(no.substring(0, 3));
		return rs == null ? 4 : rs;
	}

}
