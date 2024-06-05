package com.example.mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取 Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. 设置运行 Jar
        job.setJarByClass(SortDriver.class);

        // 3. 关联 Mapper 和 Reducer
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        // 4. 设置 Mapper 输出的 Key 和 Value
        job.setMapOutputKeyClass(KeyBean.class);
        job.setMapOutputValueClass(KeyBean.class);

        // 5. 设置最终输出的 key 和 value
        job.setOutputKeyClass(KeyBean.class);
        job.setOutputValueClass(KeyBean.class);

        // 6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("./data/input/phone_records.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./data/output/flowstat"));

        // 7. 提交 Job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0: 1);
    }
}
