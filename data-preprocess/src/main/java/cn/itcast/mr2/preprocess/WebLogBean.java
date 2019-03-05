package cn.itcast.mr2.preprocess;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 对接外部数据的层 数据的封装类
 *
 * @author: Liyuxin wechat:13011800146. @Title: WebLogBean @ProjectName hadoop-parent
 * @date: 2019/3/5 18:36
 * @description:
 */
@Data
public class WebLogBean implements Writable {

	// 194.237.142.21 - - [01/Nov/2018:06:49:18 +0000] "GET
	// /wp-content/uploads/2013/07/rstudio-git3.png HTTP/1.1" 304 0 "-" "Mozilla/4.0 (compatible;)"
	private boolean valid = true; // 判断数据是否合法
	private String remote_addr; // 记录客户端的ip地址
	private String remote_user; // 记录客户端用户名称,忽略属性"-"
	private String time_local; // 记录访问的时间
	private String request; // 记录请求的url与http协议
	private String status; // 记录请求状态 成功是200
	private String body_bytes_sent; // 记录发送给客户端文件主体内容大小
	private String http_referer; // 用来记录从哪个页面链接访问过来的
	private String http_user_agent; // 记录客户浏览器的相关信息

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.valid);
		sb.append("\001").append(this.getRemote_addr());
		sb.append("\001").append(this.getRemote_user());
		sb.append("\001").append(this.getTime_local());
		sb.append("\001").append(this.getRequest());
		sb.append("\001").append(this.getStatus());
		sb.append("\001").append(this.getBody_bytes_sent());
		sb.append("\001").append(this.getHttp_referer());
		sb.append("\001").append(this.getHttp_user_agent());

		return sb.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(this.valid);
		out.writeUTF(null == remote_addr ? "" : remote_addr);
		out.writeUTF(null == remote_user ? "" : remote_user);
		out.writeUTF(null == time_local ? "" : time_local);
		out.writeUTF(null == request ? "" : request);
		out.writeUTF(null == status ? "" : status);
		out.writeUTF(null == body_bytes_sent ? "" : body_bytes_sent);
		out.writeUTF(null == http_referer ? "" : http_referer);
		out.writeUTF(null == http_user_agent ? "" : http_user_agent);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.valid = in.readBoolean();
		this.remote_addr = in.readUTF();
		this.remote_user = in.readUTF();
		this.time_local = in.readUTF();
		this.request = in.readUTF();
		this.status = in.readUTF();
		this.body_bytes_sent = in.readUTF();
		this.http_referer = in.readUTF();
		this.http_user_agent = in.readUTF();

	}
}
