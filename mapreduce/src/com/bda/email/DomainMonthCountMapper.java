package com.bda.email;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * DomainMonthCountMapper
 *
 * Phase 3 -- Java Hadoop MapReduce batch job.
 * Intelligent College Email Classification and Opportunity Analytics System.
 *
 * Reads each line of raw/structured/emails.jsonl (one JSON object per line,
 * as emitted by parser/parse_mbox.py) and emits a composite key of
 * "<sender_domain>\t<yyyy-MM>" with value 1, for downstream aggregation
 * of "emails per sender-domain per month" (implementation_plan.md, Phase 3).
 *
 * Malformed lines and records missing sender_domain / date_utc are skipped
 * rather than failing the job, and are tracked via Hadoop Counters for
 * observability -- the dataset is known (Phase 1 audit) to contain some
 * incomplete records (Veracity).
 */
public class DomainMonthCountMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private final Text outKey = new Text();

    // Jackson's ObjectMapper is safe to reuse across map() invocations
    // within a single task (it is stateless once constructed).
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** Counters surfaced in the job's final console report for auditing. */
    public enum RecordCounter {
        MALFORMED_JSON,
        MISSING_DOMAIN,
        MISSING_DATE,
        EMITTED
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        JsonNode node;
        try {
            node = objectMapper.readTree(line);
        } catch (Exception e) {
            context.getCounter(RecordCounter.MALFORMED_JSON).increment(1);
            return;
        }

        JsonNode domainNode = node.get("sender_domain");
        JsonNode dateNode = node.get("date_utc");

        if (domainNode == null || domainNode.isNull() || domainNode.asText().isEmpty()) {
            context.getCounter(RecordCounter.MISSING_DOMAIN).increment(1);
            return;
        }
        if (dateNode == null || dateNode.isNull() || dateNode.asText().isEmpty()) {
            context.getCounter(RecordCounter.MISSING_DATE).increment(1);
            return;
        }

        String domain = domainNode.asText();
        String dateUtc = dateNode.asText(); // e.g. "2026-09-04T12:54:42+00:00"

        // yyyy-MM is always the first 7 characters of the ISO-8601 date_utc
        // string produced by parse_mbox.py's normalize_date() (guaranteed
        // format: YYYY-MM-DDTHH:MM:SS+00:00).
        if (dateUtc.length() < 7) {
            context.getCounter(RecordCounter.MISSING_DATE).increment(1);
            return;
        }
        String yearMonth = dateUtc.substring(0, 7);

        outKey.set(domain + "\t" + yearMonth);
        context.write(outKey, ONE);
        context.getCounter(RecordCounter.EMITTED).increment(1);
    }
}
