package com.example.mapreduce.serialize;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Output Key 的序列化需要完成以下操作：
 * 1. 需要实现 Hadoop 的 Writable 接口 和 Comparable 接口(因为 key 中需要进行排序)
 * 2. 重写序列化和反序列化的方法 即 write 和 read 方法(版本存在差异，名称可能不一样)。3.3.4 中是需要实现一个 read 的静态方法，
 *      接口方法是 readField
 * 3. 重写 compreTo 方法
 * 4. 重写空参构造函数
 */


public class MapperKeyBean implements Writable, Comparable {
    // 数据是读 phone_records 数据，需要保留电话号码作为 Key。本案例是为了熟悉 Key 的序列话刻意实现的
    private String phone;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public MapperKeyBean() {
    }

    @Override
    public int compareTo(Object o) {
        if (o != null){
            MapperKeyBean temp = (MapperKeyBean) o;
            return phone.compareTo(temp.phone);
        }
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(this.phone);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        phone = in.readLine();
    }

    public static MapperKeyBean run(DataInput in) throws IOException {
        MapperKeyBean bean = new MapperKeyBean();
        bean.readFields(in);

        return bean;
    }
}
