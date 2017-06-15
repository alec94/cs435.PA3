package cs435.hbaseImport;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;


/**
 * Created by Alec on 4/15/2017.
 * Mapper for hbase import, converts fields to csv format
 */
public class HBaseMapper extends Mapper<Object, Text, NullWritable, Text> {
	private final NullWritable nullKey = NullWritable.get();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] parts = value.toString().replace(",", "").split("&&", -1); // extra ',' causing hbase import to fail on some lines
		String line = "";
		if (parts.length >= 13) {
			String[] revision = parts[0].replace("REVISION", "").trim().split(" ");
			line += revision[1].trim() + ","; // revid goes first
			line += revision[0].trim() + ",";
			line += revision[2].replace("User:", "").trim() + ",";
			line += revision[3].trim() + ",";
			line += revision[4].trim() + ",";
			line += revision[5].trim() + ",";
			line += parts[1].replace("CATEGORY", "").trim() + ",";
			line += parts[2].replace("IMAGE", "").trim() + ",";
			line += parts[3].replace("MAIN", "").trim() + ",";
			line += parts[4].replace("TALK", "").trim() + ",";
			line += parts[5].replace("USER", "").trim() + ",";
			line += parts[6].replace("USER_TALK", "").trim() + ",";
			line += parts[7].replace("OTHER", "").trim() + ",";
			line += parts[8].replace("EXTERNAL", "").trim() + ",";
			line += parts[9].replace("TEMPLATE", "").trim() + ",";
			line += parts[10].replace("COMMENT", "").trim() + ",";
			line += parts[11].replace("MINOR", "").trim() + ",";
			line += parts[12].replace("TEXTDATA", "").trim();

			context.write(nullKey, new Text(line));
		} else {
			System.out.println("Discarding entry: " + Arrays.toString(parts));
		}
	}
}
