package cn.itcast.mr.demo03.flowPartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 第三步:分区,将k2按照手机号开头进行分区
 *
 * @author: Liyuxin wechat:13011800146. @Title: FlowPartition @ProjectName hadoop-parent
 * @date: 2019/2/27 17:24
 * @description:
 */
public class FlowPartition extends Partitioner<Text, FlowBean> {

  /**
   * 按照手机号开头进行分组
   *
   * @param text
   * @param flowBean
   * @param numberReduceTask
   * @return
   */
  @Override
  public int getPartition(Text text, FlowBean flowBean, int numberReduceTask) {
    String phonenum = text.toString();
    if (phonenum.startsWith("135")) {
      return 0;
    } else if (phonenum.startsWith("136")) {
      return 1;
    } else if (phonenum.startsWith("137")) {
      return 2;
    } else if (phonenum.startsWith("138")) {
      return 3;
    } else if (phonenum.startsWith("139")) {
      return 4;
    }
    return 5;
  }
}
