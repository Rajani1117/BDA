import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    private static final double DAMPING_FACTOR = 0.85;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalContribution = 0;
        String neighbors = null;

        List<String> neighborList = new ArrayList<>();

        for (Text value : values) {
            String val = value.toString();
            if (val.contains("|")) {
                // This is the original node data (rank | neighbors)
                String[] parts = val.split("\\|");
                if (parts.length == 2) {
                    neighbors = parts[1];  // Preserve the neighbor list
                }
            } else {
                // This is a rank contribution
                totalContribution += Double.parseDouble(val);
            }
        }

        // Compute new rank
        double newRank = (1 - DAMPING_FACTOR) + DAMPING_FACTOR * totalContribution;

        // Output the updated rank along with its neighbors
        if (neighbors != null) {
            context.write(key, new Text(newRank + "|" + neighbors));
        }
    }
}
