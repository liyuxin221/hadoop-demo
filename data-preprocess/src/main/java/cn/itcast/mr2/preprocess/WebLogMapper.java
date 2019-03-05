package cn.itcast.mr2.preprocess;

import cn.itcast.mr2.Utils.WebLogParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author: Liyuxin wechat:13011800146. @Title: WebLogMapper @ProjectName hadoop-parent
 * @date: 2019/3/5 19:32
 * @description:
 */
public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	Set<String> pages = new HashSet<String>();
	Text k = new Text();
	NullWritable v = NullWritable.get();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		pages.add("/about");
		pages.add("/black-ip-list/");
		pages.add("/cassandra-clustor/");
		pages.add("/finance-rhive-repurchase/");
		pages.add("/hadoop-family-roadmap/");
		pages.add("/hadoop-hive-intro/");
		pages.add("/hadoop-zookeeper-intro/");
		pages.add("/hadoop-mahout-roadmap/");
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String s = value.toString();
		WebLogBean bean = WebLogParser.parser(s);

		if (bean != null) {
			// 过滤静态页面
			WebLogParser.filtStaticResource(bean, pages);

			k.set(bean.toString());
			context.write(k, v);
		}
	}
}
