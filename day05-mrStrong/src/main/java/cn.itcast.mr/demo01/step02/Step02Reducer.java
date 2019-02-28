package cn.itcast.mr.demo01.step02;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: Step02Reducer @ProjectName hadoop-parent
 * @date: 2019/2/28 17:36
 * @description:
 */
public class Step02Reducer extends Reducer<Text, Text, Text, Text> {

	private StringBuffer commonFriendsBuffer;
	private Text commonFriendText;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		commonFriendsBuffer = new StringBuffer();
		commonFriendText = new Text();
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		commonFriendsBuffer.setLength(0);

		for (Text value : values) {
			commonFriendsBuffer.append(value).append(",");
		}
		commonFriendText.set(commonFriendsBuffer.toString());

		context.write(key, commonFriendText);
	}
}
