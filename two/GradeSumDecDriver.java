package cn.edu.gdpu.lastwork.two;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class GradeSumDecDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String input = args[0];
        String output = args[1];
        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(GradeSumDecMapper.class);
        //2.设置mapper类
        job.setMapperClass(GradeSumDecMapper.class);
        job.setReducerClass(GradeSumDecReducer.class);
        //3.设置输入map key v
        job.setMapOutputKeyClass(GradeBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(GradeSumDecMapper.class);
        job.setOutputValueClass(NullWritable.class);
        //设置combineTextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);
        job.setPartitionerClass(GradePartitioner.class);
        job.setNumReduceTasks(3);
        //**LazyOutputFormat，Reducer将不再生成空的输出文件
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        //5.设置InputFormat
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        //提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
