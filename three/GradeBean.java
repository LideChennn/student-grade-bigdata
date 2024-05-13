package cn.edu.gdpu.lastwork.three;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GradeBean implements WritableComparable<GradeBean> {//每一条数据
    private String subject; //标识哪个科目，这个字段在进入partitioner时，用于区分分区
    private int score;
    private String examId;
    private String name;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(subject);
        out.writeInt(score);
        out.writeUTF(examId);
        out.writeUTF(name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        subject = in.readUTF();
        score = in.readInt();
        examId = in.readUTF();
        name = in.readUTF();
    }

    @Override
    public int compareTo(GradeBean o) {
        int compare = Integer.compare(o.score, this.score);//分数倒序排序
        return compare == 0 ? this.examId.compareTo(o.examId) : compare;
    }

    @Override
    public String toString() {
        return examId + "\t" + name + "\t" + subject + "\t" + score;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public String getExamId() {
        return examId;
    }

    public void setExamId(String examId) {
        this.examId = examId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
