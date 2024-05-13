package cn.edu.gdpu.lastwork.four;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GradeAvgDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String input = args[0];
        String output = args[1];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(GradeAvgDriver.class);
        job.setMapperClass(GradeAvgMapper.class);
        job.setReducerClass(GradeAvgReducer.class);
        job.setCombinerClass(GradeCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumAndCountBean.class);
        //设置combineTextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 设置最大切片大小（以字节为单位），例如：设置为4MB
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
