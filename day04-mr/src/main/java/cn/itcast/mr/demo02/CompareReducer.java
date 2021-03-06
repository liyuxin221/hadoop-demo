package cn.itcast.mr.demo02;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 将k3写入文件即可 v3为null
 * @author: Liyuxin wechat:13011800146.
 * @Title: PartitionReducer
 * @ProjectName hadoop-parent
 * @date: 2019/2/26 19:30
 * @description:
 */
public class CompareReducer extends Reducer<PairTwo, NullWritable,PairTwo,NullWritable> {
	@Override
	protected void reduce(PairTwo key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

		context.write(key,NullWritable.get());
	}
}
