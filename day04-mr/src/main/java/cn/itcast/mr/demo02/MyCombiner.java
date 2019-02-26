package cn.itcast.mr.demo02;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义规约类
 * @author: Liyuxin wechat:13011800146.
 * @Title: MyComparor
 * @ProjectName hadoop-parent
 * @date: 2019/2/26 21:51
 * @description:
 */
public class MyCombiner extends Reducer<PairTwo, NullWritable,PairTwo,NullWritable> {

	@Override
	protected void reduce(PairTwo key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		context.write(key,NullWritable.get());
	}
}
