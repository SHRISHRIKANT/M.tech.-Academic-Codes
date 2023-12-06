package org.shrikant;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class mapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Assuming the input format is comma-separated
        String[] fields = value.toString().split(",");

        if (fields.length >= 15 && !fields[1].isEmpty() && !fields[14].isEmpty() && !fields[6].isEmpty()) {
            String directorName = fields[1].trim();
            String imdbScore = fields[14].trim();
            String movieTitle = fields[6].trim();

            try {
                outputKey.set(directorName);
                outputValue.set(imdbScore + ":" + movieTitle);
                context.write(outputKey, outputValue);
            } catch (NumberFormatException e) {
                // Handle parsing errors
            }
        }
    }
}