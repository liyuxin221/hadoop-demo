package cn.itcast.mr.demo02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * k1 v1
 * k2 v2 一行文本,null
 * @author: Liyuxin wechat:13011800146.
 * @Title: PartitionMapper
 * @ProjectName hadoop-parent
 * @date: 2019/2/26 19:30
 * @description:
 */
public class CompareMapper extends Mapper<LongWritable, Text,PairTwo, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] splits = value.toString().split("\t");
		PairTwo pairTwo = new PairTwo();

		pairTwo.setFirst(splits[0]);
		pairTwo.setSecond(Integer.parseInt(splits[1]));

		context.write(pairTwo,NullWritable.get());

	}
}
