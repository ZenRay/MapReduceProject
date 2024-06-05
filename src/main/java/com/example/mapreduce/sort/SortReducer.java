package com.example.mapreduce.sort;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<KeyBean, KeyBean, KeyBean, KeyBean> {
    private KeyBean outValue = new KeyBean();

    @Override
    protected void reduce(
            KeyBean key, Iterable<KeyBean> values, Reducer<KeyBean, KeyBean, KeyBean, KeyBean>.Context context
    ) throws IOException, InterruptedException {
        // 需要按照电话号码的 Key 解决统计上行、下行流量以及总流量
        long totalUp = 0;
        long totalDown = 0;

        // 计算数据
        for (KeyBean value : values) {
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();
        }

        // 封装数据
        outValue.setUpFlow(totalUp);
        outValue.setDownFlow(totalDown);
        outValue.setTotalFlow();

        // 保存数据
        context.write(key, outValue);

    }
}
