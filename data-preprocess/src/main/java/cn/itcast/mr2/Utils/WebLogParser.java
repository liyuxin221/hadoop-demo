package cn.itcast.mr2.Utils;

import cn.itcast.mr2.preprocess.WebLogBean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Set;

/**
 * @author: Liyuxin wechat:13011800146. @Title: WebLogParser @ProjectName hadoop-parent
 * @date: 2019/3/5 18:59
 * @description:
 */
public class WebLogParser {

	public static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
	public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

	public static WebLogBean parser(String line) {
		WebLogBean webLogBean = new WebLogBean();

		// 194.237.142.21 - - [01/Nov/2018:06:49:18 +0000] "GET
		// /wp-content/uploads/2013/07/rstudio-git3.png HTTP/1.1" 304 0 "-" "Mozilla/4.0 (compatible;)"
		String[] arr = line.split(" ");
		if (arr.length > 11) {
			webLogBean.setRemote_addr(arr[0]);
			webLogBean.setRemote_user(arr[1]);
			String time_local = formatDate(arr[3].substring(1));
			if (time_local == null || time_local == "") {
				time_local = "-invalid_time-";
			}
			webLogBean.setTime_local(time_local);
			webLogBean.setRequest(arr[6]);
			webLogBean.setStatus(arr[8]);
			webLogBean.setBody_bytes_sent(arr[9]);
			webLogBean.setHttp_referer(arr[10]);

			if (arr.length > 12) {
				StringBuffer sb = new StringBuffer();
				for (int i = 11; i < arr.length; i++) {
					sb.append(arr[i]);
				}
				webLogBean.setHttp_user_agent(sb.toString());
			} else {
				webLogBean.setHttp_user_agent(arr[11]);
			}

		} else {
			webLogBean = null;
		}

		return webLogBean;
	}

	private static String formatDate(String time_local) {
		try {
			return df2.format(df1.parse(time_local));
		} catch (ParseException e) {
			return null;
		}
	}

	public static void filtStaticResource(WebLogBean bean, Set<String> pages) {
		if (!pages.contains(bean.getRequest())) {
			bean.setValid(false);
		}
	}
}
