package cs435.hbaseImport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Alec on 4/15/2017.
 * main driver for map reduce algorithm
 */
public class Main {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length < 2) {
			System.out.println("ERROR: Wrong number of arguments\nUSAGE: <input file> <output folder>");
			System.exit(-1);
		}

		System.exit(run(new Path(args[0]), new Path(args[1])));
	}

	private static int run(Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);

		Job job = Job.getInstance(configuration);
		job.setJarByClass(Main.class);
		job.setMapperClass(HBaseMapper.class);
		//job.setNumReduceTasks(0);
		//job.setReducerClass(HBaseReducer.class);
		job.setInputFormatClass(WikiHBaseInputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		if (fileSystem.exists(output)) {
			fileSystem.delete(output, true);
		}

		return (job.waitForCompletion(true) ? 1 : 0);
	}
}


