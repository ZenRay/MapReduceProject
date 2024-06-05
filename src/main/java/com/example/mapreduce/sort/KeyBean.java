package com.example.mapreduce.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 需要对总流量进行排序，因此需要修改一下将流量的 Bean 当作 key 来使用进行排序
 */
public class KeyBean implements WritableComparable<KeyBean> {
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
    public KeyBean() {
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

    public static KeyBean read(DataInput in) throws IOException {
        KeyBean bean = new KeyBean();
        bean.readFields(in);

        return bean;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + totalFlow;
    }
}