package com.example.mapreduce.partition;


import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 需求是根据手机号放到不同的文件中，因此需要重写分区类
 */
public class ProvinePartition extends Partitioner<KeyBean, ValueBean> {
    @Override
    public int getPartition(KeyBean keyBean, ValueBean valueBean, int numPartitions) {
        String phone = keyBean.getPhone();
        int partition;

        if ("135".equals(phone.substring(0, 3))){
            partition = 0;
        } else if ("136".equals(phone.substring(0, 3))) {
            partition = 1;
        } else if ("137".equals(phone.substring(0, 3))) {
            partition = 2;
        } else if ("138".equals(phone.substring(0, 3))){
            partition = 3;
        } else {
            partition = 4;
        }
        return partition;
    }
}
