package cn.leo.mapred.test.join;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReduce extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String changshang = "";
		Set<String> chanpingSet = new HashSet<String>();
		for (Text text : values) {
			String str = text.toString();
			if (str.contains("-b.txt")) {
				changshang = str.substring(0, str.indexOf("-"));
			} else {
				chanpingSet.add(str.substring(0, str.indexOf("-")));
			}
		}

		if (chanpingSet.size() < 1) {
			context.write(new Text(""), new Text(changshang));
		} else {
			for (String chanping : chanpingSet) {
				context.write(new Text(chanping), new Text(changshang));
			}
		}

	}
}
