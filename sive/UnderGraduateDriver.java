package cn.edu.gdpu.lastwork.sive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class UnderGraduateDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        String cachePath = args[0];//要five运行的输出文件
        String input =args[1];
        String output = args[2];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnderGraduateDriver.class);
        //不设置reducer会走默认的reducer。
        job.setMapperClass(UnderGraduateMapper.class);
        job.setReducerClass(UnderGraduateReducer.class);
        job.setMapOutputKeyClass(GradeBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(GradeBean.class);
        job.setOutputValueClass(NullWritable.class);
        //设置combineTextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 设置最大切片大小（以字节为单位），例如：设置为4MB
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);
        job.setPartitionerClass(UnderGraduatePartitioner.class); //分区
        job.setNumReduceTasks(3);
        //加载缓存文件
        job.addCacheFile(new URI(cachePath));
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
