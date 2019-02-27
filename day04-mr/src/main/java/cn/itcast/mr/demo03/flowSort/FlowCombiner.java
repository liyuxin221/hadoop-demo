package cn.itcast.mr.demo03.flowSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * 规约
 * @author: Liyuxin wechat:13011800146.
 * @Title: FlowCombiner
 * @ProjectName hadoop-parent
 * @date: 2019/2/26 23:40
 * @description:
 */
public class FlowCombiner extends Reducer<FlowBean,Text, FlowBean,Text> {
	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    for (Text value : values) {
		//
		context.write(key,value);
    }
	}
}
