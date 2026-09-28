package com.bda.email;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * DomainMonthCountDriver
 *
 * Phase 3 job entry point -- "emails per sender-domain per month"
 * (implementation_plan.md, Phase 3).
 *
 * Input:  raw/structured/emails.jsonl on HDFS (JSON Lines, read via
 *         TextInputFormat -- no Parquet/Avro JARs required).
 * Output: results/domain_month_counts/ on HDFS -- tab-separated
 *         "<domain>\t<yyyy-MM>\t<count>" rows, one per reducer key.
 *
 * Usage:
 *   hadoop jar domain-month-count.jar com.bda.email.DomainMonthCountDriver \
 *       <input path on HDFS> <output path on HDFS>
 *
 * Example:
 *   hadoop jar domain-month-count.jar com.bda.email.DomainMonthCountDriver \
 *       /user/student/email_project/raw/structured/emails.jsonl \
 *       /user/student/email_project/results/domain_month_counts
 */
public class DomainMonthCountDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DomainMonthCountDriver <input path> <output path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Emails per Sender-Domain per Month");
        job.setJarByClass(DomainMonthCountDriver.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(DomainMonthCountMapper.class);
        job.setCombinerClass(DomainMonthCountReducer.class);
        job.setReducerClass(DomainMonthCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
