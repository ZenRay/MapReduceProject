package com.example.mapreduce.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Output Value 的序列化需要完成以下操作：
 * 1. 需要实现 Hadoop 的 Writable 接口
 * 2. 重写序列化和反序列化的方法 即 write 和 read 方法(版本存在差异，名称可能不一样)。3.3.4 中是需要实现一个 read 的静态方法，
 *      接口方法是 readField
 * 3. 重写空参构造函数
 */
public class ValueBean implements Writable {
    // 数据是读 phone_records 数据，需要保留上行、下行流量以及总流量。保存为成员变量 作为结果值
    private long upFlow;
    private long downFlow;
    private long totalFlow;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getTotalFlow() {
        if (totalFlow == 0){
            return this.downFlow + this.upFlow;
        }
        return this.totalFlow;


    }

    public void setTotalFlow() {
        this.totalFlow = this.upFlow + this.downFlow;
    }

    // 空参构造
    public ValueBean() {
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.upFlow);
        out.writeLong(this.downFlow);
        out.writeLong(this.getTotalFlow());

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.totalFlow = in.readLong();
    }

    public static ValueBean read(DataInput in) throws IOException {
        ValueBean bean = new ValueBean();
        bean.readFields(in);

        return bean;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + totalFlow;
    }
}
