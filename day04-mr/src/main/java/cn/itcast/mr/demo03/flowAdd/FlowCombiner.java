package cn.itcast.mr.demo03.flowAdd;

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
public class FlowCombiner extends Reducer<Text,FlowBean,Text,FlowBean> {
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

		int upFlow = 0;
		int downFlow = 0;
		int upCountFlow = 0;
		int downCountFlow = 0;

		for (FlowBean value : values) {
			// 求和
			upFlow += value.getUpFlow();
			downFlow += value.getDownFlow();
			upCountFlow += value.getUpCountFlow();
			downCountFlow += value.getDownCountFlow();
		}

		FlowBean flowBean = new FlowBean();
		flowBean.setUpFlow(upFlow);
		flowBean.setDownFlow(downFlow);
		flowBean.setUpCountFlow(upCountFlow);
		flowBean.setDownCountFlow(downCountFlow);

		context.write(key,flowBean);
	}

}
