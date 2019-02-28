package demo01.step01;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: Step1Combiner @ProjectName hadoop-parent
 * @date: 2019/2/28 16:39
 * @description:
 */
public class Step1Combiner extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			//
			context.write(key, value);
		}
	}
}
