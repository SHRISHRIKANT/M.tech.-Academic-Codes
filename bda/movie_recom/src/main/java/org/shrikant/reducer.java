package org.shrikant;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
public class reducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0.0;
        int count = 0;
        double maxRating = Double.MIN_VALUE;
        String maxRatedMovie = "";
        for (Text value : values) {
            String[] parts = value.toString().split(":");
            if (parts.length == 2) {
                try {
                    double rating = Double.parseDouble(parts[0]);
                    sum += rating;
                    count++;
                    if (rating > maxRating) {
                        maxRating = rating;
                        maxRatedMovie = parts[1];
                    }
                } catch (NumberFormatException e) {
                }
            }
        }
        if (count > 0) {
            double averageRating = sum / count;
            result.set("Average Rating: " + averageRating + ", Highest Rated Movie: " + maxRatedMovie);
            context.write(key, result);
        }
    }
}
