package demo01.step01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: Step1Mapper @ProjectName hadoop-parent
 * @date: 2019/2/28 16:38
 * @description:
 */
public class Step1Mapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text friend;
	private Text user;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		friend = new Text();
		user = new Text();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split(":");
		String user = split[0];
		this.friend.set(friend);

		String friends = split[1];
		String[] split1 = friends.split(",");

		for (String friend : split1) {
			this.user.set(user);
			context.write(this.friend, this.user);
		}
	}
}
