package com.example.mapreduce.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SerializeDriver{

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取 Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. 设置运行 Jar
        job.setJarByClass(SerializeDriver.class);

        // 3. 关联 Mapper 和 Reducer
        job.setMapperClass(SerializeMapper.class);
        job.setReducerClass(SerializeReducer.class);

        // 4. 设置 Mapper 输出的 Key 和 Value
        job.setMapOutputKeyClass(KeyBean.class);
        job.setMapOutputValueClass(ValueBean.class);

        // 5. 设置最终输出的 key 和 value
        job.setOutputKeyClass(KeyBean.class);
        job.setOutputValueClass(ValueBean.class);

        // 设置定制化的 partition，且需要设置的 partition 超过 1
        job.setPartitionerClass(ProvinePartition.class);
        job.setNumReduceTasks(5);


        // 6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("./data/input/phone_records.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./data/output/partition"));

        // 7. 提交 Job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0: 1);
    }
}
