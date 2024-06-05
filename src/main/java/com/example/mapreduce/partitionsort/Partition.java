package com.example.mapreduce.partitionsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partition extends Partitioner<ValueBean, Text> {

    @Override
    public int getPartition(ValueBean valueBean, Text text, int numPartitions) {
        /**
         * 进行 Debug 时发现 text 的之中存在 \u0000 的值进行分割的数据
         */
        StringBuilder stringBuilder = new StringBuilder();
        String phone = text.toString();
        for(int i=0; i < phone.length(); i++){
            if(phone.charAt(i) != '\u0000'){
                stringBuilder.append(phone.charAt(i));
            }
        }

        phone = stringBuilder.toString();


        String prePhone = phone.substring(0, 3);

        int partition;

        if ("135".equals(prePhone)){
            partition = 0;
        } else if ("136".equals(prePhone)) {
            partition = 1;
        } else if ("137".equals(prePhone)) {
            partition = 2;
        } else if ("138".equals(prePhone)) {
            partition = 3;
        } else {
            partition = 4;
        }

        return partition;

    }
}
