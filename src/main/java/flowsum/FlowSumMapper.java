package flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // get a line
        String line = value.toString();

        // split many fileds
        String[] fidlds = StringUtils.split(line, '\t');

        String phoneNB = fidlds[1];
        long u_flow = Long.parseLong(fidlds[7]);
        long d_flow = Long.parseLong(fidlds[8]);

        context.write(new Text(phoneNB), new FlowBean(phoneNB, u_flow, d_flow));


    }
}
