package com.example.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CustomizeOutputFormat extends FileOutputFormat<Text, NullWritable> {
    FSDataOutputStream targetOutputFormat;
    FSDataOutputStream otherOutputFormat;

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws InterruptedException {
        // Path outputPath = FileOutputFormat.getOutputPath(job);
        // 需要根据数据内容创建流信息
        try {
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());

            targetOutputFormat = fileSystem.create(new Path("./data/output/outputformat/target.txt"));
            otherOutputFormat = fileSystem.create(new Path("./data/output/outputformat/other.txt"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        return new RecordWriter<Text, NullWritable>() {
            @Override
            public void write(Text key, NullWritable value) throws IOException, InterruptedException {
                String line = key.toString();

                // 根据内容判断是否写入到目标的流中
                if (line.contains("sina")){
                    targetOutputFormat.writeBytes(line + "\n");
                } else{
                    otherOutputFormat.writeBytes(line + "\n");
                }

            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                IOUtils.closeStream(targetOutputFormat);
                IOUtils.closeStream(otherOutputFormat);
            }
        };

    }
}
