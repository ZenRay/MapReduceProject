package com.example.mapreduce.partitionsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 结果上因为需要用电话号码作为 key 信息，因此在结果上使用了 Text 作为 Reduce 的 Key
 * 在输入。本次调用已经计算出来的流量结果进行排序
 */
public class PartitionSortReducer extends Reducer<ValueBean, Text, Text, ValueBean> {

    @Override
    protected void reduce(ValueBean key, Iterable<Text> values, Reducer<ValueBean, Text, Text, ValueBean>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
