package demo01.step01;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: Step1Reducer @ProjectName hadoop-parent
 * @date: 2019/2/28 16:39
 * @description:
 */
public class Step1Reducer extends Reducer<Text, Text, Text, Text> {

	private StringBuffer userStringBuffer;
	private Text userText;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		userStringBuffer = new StringBuffer();
		userText = new Text();
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// 清空StringBuffer
		userStringBuffer.setLength(0);

		for (Text value : values) {
			userStringBuffer.append(value);
		}
		userText.set(userStringBuffer.toString());

		context.write(key, userText);
	}
}
