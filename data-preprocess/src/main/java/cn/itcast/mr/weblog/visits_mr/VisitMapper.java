package cn.itcast.mr.weblog.visits_mr;

import cn.itcast.mr.weblog.pageviews.PageViewsBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VisitMapper extends Mapper<LongWritable, Text, Text, PageViewsBean> {
	private Text textKey = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		PageViewsBean bean = new PageViewsBean();
		setter(line, bean);
		context.write(textKey, bean);

	}

	public void setter(String line, PageViewsBean bean) {
		//2f0a9bef-5ed4-44bd-bd4e-24017d9497f9
		//1.80.249.223
		// -
		// 2018-11-01 07:57:33     3
		// /hadoop-hive-intro/      4
		// 1  step
		// 60 staylong
		// "http://www.google.com.hk/url?sa=t&rct=j&q=hive%E7%9A%84%E5%AE%89%E8%A3%85&source=web&cd=2&ved=0CC4QFjAB&url=%68%74%74%70%3a%2f%2f%62%6c%6f%67%2e%66%65%6e%73%2e%6d%65%2f%68%61%64%6f%6f%70%2d%68%69%76%65%2d%69%6e%74%72%6f%2f&ei=5lw5Uo-2NpGZiQfCwoG4BA&usg=AFQjCNF8EFxPuCMrm7CvqVgzcBUzrJZStQ&bvm=bv.52164340,d.aGc&cad=rjt"
		// "Mozilla/5.0 (Windows NT 5.2; rv:23.0) Gecko/20100101 Firefox/23.0" 
		// 14764 byte
		// 200 status
		String[] split = line.split("\001");
		textKey.set(split[0]);

		bean.setSession(split[0]);
		bean.setRemote_addr(split[1]);
		bean.setTimestr(split[3]);
		bean.setRequest(split[4]);
		bean.setStep(Integer.parseInt(split[5]));
		bean.setStaylong(split[6]);
		bean.setReferal(split[7]);
		bean.setUseragent(split[8]);
		bean.setBytes_send(split[9]);
		bean.setStatus(split[10]);
	}
}
