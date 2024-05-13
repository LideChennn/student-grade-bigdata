package cn.edu.gdpu.lastwork.three;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GradeTop100Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String rank = args[0];//top 100
        String input = args[1];
        String output = args[2];
        Configuration conf = new Configuration();
        conf.set("rank", rank);//Topx
        Job job = Job.getInstance(conf);
        job.setJarByClass(GradeTop100Mapper.class);
        //2.设置mapper类
        job.setMapperClass(GradeTop100Mapper.class);
        job.setReducerClass(GradeTop100Reducer.class);
        //3.设置输入map key v
        job.setMapOutputKeyClass(GradeBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //4.设置最终输出的kv
        job.setOutputKeyClass(GradeTop100Mapper.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);
        job.setPartitionerClass(GradePartitioner.class);
        job.setNumReduceTasks(6);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        //提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
