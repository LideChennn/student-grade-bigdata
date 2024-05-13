package cn.edu.gdpu.lastwork.three;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GradePartitioner extends Partitioner<GradeBean, NullWritable> {

    @Override
    public int getPartition(GradeBean gradeBean, NullWritable nullWritable, int i) {
        String subject = gradeBean.getSubject();
        int part = 0;
        switch (subject) {
            case "语文":
                break;
            case "数学":
                part = 1;
                break;
            case "英语":
                part = 2;
                break;
            case "政治":
                part = 3;
                break;
            case "生物":
                part = 4;
                break;
            case "物理":
                part = 5;
                break;
        }
        return part;
    }
}
