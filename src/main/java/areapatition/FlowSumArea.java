package areapatition;

import flowsum.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSumArea {
    public static class FlowSumAreaMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");

            String phoneNB = fields[0];
            long u_flow = Long.parseLong(fields[7]);
            long d_flow = Long.parseLong(fields[8]);

            context.write(new Text(phoneNB), new FlowBean(phoneNB, u_flow, d_flow));
        }
    }

    public static class FlowSumAreaReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long u_flow_count = 0;
            long d_flow_count = 0;

            for (FlowBean bean : values) {
                u_flow_count += bean.getUp_flow();
                d_flow_count += bean.getD_flow();
            }
            context.write(key, new FlowBean(key.toString(), u_flow_count, d_flow_count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // 这里会去读yarn-site.xml
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowSumArea.class);
        job.setMapperClass(FlowSumAreaMapper.class);
        job.setReducerClass(FlowSumAreaReducer.class);
        job.setPartitionerClass(AreaPaetitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setNumReduceTasks(6);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
