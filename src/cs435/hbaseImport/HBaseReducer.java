package cs435.hbaseImport;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Alec on 4/15/2017.
 * reducer for hbase import, inserts csv fields into hbase table
 */
public class HBaseReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(key, value);
		}
	}

}
