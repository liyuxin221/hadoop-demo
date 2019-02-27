package cn.itcast.mr.demo03.flowSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146.
 * @Title: FlowMapper
 * @ProjectName hadoop-parent
 * @date: 2019/2/26 23:29
 * @description:
 */
public class FlowMapper extends Mapper<LongWritable, Text,FlowBean,Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] splits = value.toString().split("\t");

		FlowBean flowBean = new FlowBean();
		flowBean.setUpFlow(Integer.parseInt(splits[1]));
		flowBean.setDownFlow(Integer.parseInt(splits[2]));
		flowBean.setUpCountFlow(Integer.parseInt(splits[3]));
		flowBean.setDownCountFlow(Integer.parseInt(splits[4]));

		context.write(flowBean,new Text(splits[0]));
	}
}
