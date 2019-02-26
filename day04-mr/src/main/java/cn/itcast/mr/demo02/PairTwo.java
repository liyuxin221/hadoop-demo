package cn.itcast.mr.demo02;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: PairTwo @ProjectName hadoop-parent
 * @date: 2019/2/26 21:39
 * @description:
 */
@Data
public class PairTwo implements WritableComparable<PairTwo> {

  private String first;
  private Integer second;

  /**
   * @param o the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
   *     or greater than the specified object.
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException if the specified object's type prevents it from being compared to
   *     this object.
   */
  @Override
  public int compareTo(PairTwo o) {
    int result = this.first.compareTo(o.first);
    if (result == 0) {
      result = this.second.compareTo(o.second);
    }
    return result;
  }

  // 序列化
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(first);
    dataOutput.writeInt(second);
  }

  // 反序列化
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.first = dataInput.readUTF();
    this.second = dataInput.readInt();
  }

  @Override
  public String toString() {
    return first + "\t" + second;
  }
}
