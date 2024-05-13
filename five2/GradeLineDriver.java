package cn.edu.gdpu.lastwork.five2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GradeLineDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String undergraduateRate = "0.6";
        String inputPath = "E:\\HomeWork\\大三下\\JAVAEE\\workspace\\javaee_review\\MapReduceDemo\\src\\main\\resources\\lastworkoutputtwo";
        String outputPath ="E:\\HomeWork\\大三下\\JAVAEE\\workspace\\javaee_review\\MapReduceDemo\\src\\main\\resources\\lastworksix2";
//        String undergraduateRate = args[0];//本科率，小数
//        String inputPath = args[1];
//        String outputPath = args[2];

        Configuration conf = new Configuration();
        conf.set("undergraduateRate", undergraduateRate);//本科率

        Job job = Job.getInstance(conf);

        job.setJarByClass(GradeLineDriver.class);

        job.setMapperClass(GradeLineMapper.class);
        job.setReducerClass(GradeLineReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置combineTextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 设置最大切片大小（以字节为单位），例如：设置为4MB
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
