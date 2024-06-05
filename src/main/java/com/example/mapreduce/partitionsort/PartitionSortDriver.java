package com.example.mapreduce.partitionsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 本次仅进行了排序，因此我们直接调用计算出来的流量结果数据作为输入。计算的输入需要调用流量统计的结果作为输入，需要删除其他非流量结果数据
 * 因为进行了分区排序，所以需要设置分区类信息
 */
public class PartitionSortDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取 Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. 设置运行 Jar
        job.setJarByClass(PartitionSortDriver.class);

        // 3. 关联 Mapper 和 Reducer
        job.setMapperClass(PartitionSortMapper.class);
        job.setReducerClass(PartitionSortReducer.class);

        // 4. 设置 Mapper 输出的 Key 和 Value
        job.setMapOutputKeyClass(ValueBean.class);
        job.setMapOutputValueClass(Text.class);

        // 5. 设置最终输出的 key 和 value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ValueBean.class);

        job.setPartitionerClass(Partition.class);
        job.setNumReduceTasks(5);

        // 6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("./data/output/flowstat"));
        FileOutputFormat.setOutputPath(job, new Path("./data/output/partitionsort"));

        // 7. 提交 Job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0: 1);
    }
}
