package cn.itcast.mr.demo01.step02;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146.
 * @Title: Step02Combiner
 * @ProjectName hadoop-parent
 * @date: 2019/2/28 17:36
 * @description:
 */
public class Step02Combiner extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(key, value);
		}
	}
}
