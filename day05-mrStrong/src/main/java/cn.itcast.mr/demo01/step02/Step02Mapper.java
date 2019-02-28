package cn.itcast.mr.demo01.step02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author: Liyuxin wechat:13011800146. @Title: Step02Mapper @ProjectName hadoop-parent
 * @date: 2019/2/28 17:36
 * @description:
 */
public class Step02Mapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text commonUser;
	private Text friend;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		commonUser = new Text();
		friend = new Text();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split("\t");
		String friend = split[0];
		this.friend.set(friend);

		String users = split[1];

		String[] usersArr = users.split("-");

		Arrays.sort(usersArr);

		for (int i = 0; i < usersArr.length - 1; i++) {
			for (int j = i + 1; j < usersArr.length; j++) {

				commonUser.set(usersArr[i] + "-" + usersArr[j]);

				context.write(commonUser, this.friend);
			}
		}
	}
}
