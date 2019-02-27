package cn.itcast.mr.demo03.flowSort;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: FlowBean @ProjectName hadoop-parent
 * @date: 2019/2/26 23:29
 * @description:
 */
@Data
public class FlowBean implements WritableComparable<FlowBean> {
  private Integer upFlow;
  private Integer downFlow;
  private Integer upCountFlow;
  private Integer downCountFlow;

  @Override
  public int compareTo(FlowBean o) {
    int result = -this.upFlow.compareTo(o.upFlow);
    if (result==0){
      result = -this.downFlow.compareTo(o.downFlow);
      if (result == 0) {
        result = -this.upCountFlow.compareTo(o.upCountFlow);
        if (result == 0) {
          result = -this.downCountFlow.compareTo(o.downCountFlow);
        }
      }
    }
    return result;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(upFlow);
    out.writeInt(downFlow);
    out.writeInt(upCountFlow);
    out.writeInt(downCountFlow);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.upFlow = in.readInt();
    this.downFlow = in.readInt();
    this.upCountFlow = in.readInt();
    this.downCountFlow = in.readInt();
  }

  @Override
  public String toString() {
    return upFlow + "\t" + downFlow + "\t" + upCountFlow + "\t" + downCountFlow;
  }
}
