package cn.itcast.mr.weblog.visits_mr;

import cn.itcast.mr.weblog.pageviews.PageViewsBean;
import cn.itcast.mr.weblog.visits.VisitBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

public class VisitReducer extends Reducer<Text, PageViewsBean, NullWritable, VisitBean> {
	private NullWritable nullWritable = NullWritable.get();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	protected void reduce(Text key, Iterable<PageViewsBean> values, Context context) throws IOException, InterruptedException {
		ArrayList<PageViewsBean> list = new ArrayList<>();

		for (PageViewsBean value : values) {
			PageViewsBean bean = new PageViewsBean();
			try {
				BeanUtils.copyProperties(bean, value);
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
			list.add(bean);
		}

		Collections.sort(list, new Comparator<PageViewsBean>() {

			@Override
			public int compare(PageViewsBean o1, PageViewsBean o2) {

				return o1.getStep() > o2.getStep() ? 1 : -1;
			}
		});

		// 取这次visit的首尾pageview记录，将数据放入VisitBean中
		VisitBean visitBean = new VisitBean();
		// 取visit的首记录
		visitBean.setInPage(list.get(0).getRequest());
		visitBean.setInTime(list.get(0).getTimestr());
		// 取visit的尾记录
		visitBean.setOutPage(list.get(list.size() - 1).getRequest());

		String time = list.get(list.size() - 1).getTimestr();
		try {
			Date parse = sdf.parse(time);
			long time1 = parse.getTime();
			time1 += 60 * 1000;
			parse = new Date(time1);
			time = sdf.format(parse);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		visitBean.setOutTime(time);
		// visit访问的页面数
		visitBean.setPageVisits(list.size());
		// 来访者的ip
		visitBean.setRemote_addr(list.get(0).getRemote_addr());
		// 本次visit的referal
		visitBean.setReferal(list.get(0).getReferal());
		visitBean.setSession(key.toString());

		context.write(NullWritable.get(), visitBean);

	}

}
