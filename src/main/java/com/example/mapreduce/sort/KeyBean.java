package com.example.mapreduce.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 需要对总流量进行排序，因此需要修改一下将流量的 Bean 当作 key 来使用进行排序——排序方案是倒排
 */

public class KeyBean implements WritableComparable<KeyBean> {
    // 数据是读 phone_records 数据，需要保留电话号码作为 Key。本案例是为了熟悉 Key 的序列话刻意实现的
    private String phone;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public KeyBean() {
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(this.phone);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        phone = in.readLine();
    }

    public static com.example.mapreduce.serialize.KeyBean run(DataInput in) throws IOException {
        com.example.mapreduce.serialize.KeyBean bean = new com.example.mapreduce.serialize.KeyBean();
        bean.readFields(in);

        return bean;
    }

    @Override
    public String toString() {
        return phone;
    }


    @Override
    public int compareTo(KeyBean o) {
        return phone.compareTo(o.getPhone());
    }
}
