package cn.edu.gdpu.lastwork.one;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GradeSumDecDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        String input = "E:\\HomeWork\\大三下\\JAVAEE\\workspace\\javaee_review\\MapReduceDemo\\src\\main\\resources\\lastwork";
//        String output = "E:\\HomeWork\\大三下\\JAVAEE\\workspace\\javaee_review\\MapReduceDemo\\src\\main\\resources\\lastworkoutput";
        String input = args[0];
        String output = args[1];
        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 开启map端输出压缩，支持切片
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class,CompressionCodec.class);
        job.setJarByClass(GradeSumDecMapper.class);
        //2.设置mapper类
        job.setMapperClass(GradeSumDecMapper.class);
        //3.设置输入map key v
        job.setMapOutputKeyClass(GradeBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //4.设置最终输出的kv
        job.setOutputKeyClass(GradeBean.class);
        job.setOutputValueClass(NullWritable.class);
        //设置combineTextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 设置最大切片大小（以字节为单位），例如：设置为4MB
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);
        //5.设置InputFormat
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        //提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
