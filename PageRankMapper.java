import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split("\t");

        if (parts.length != 2) return;

        String node = parts[0];
        String[] data = parts[1].split("\\|");
        if (data.length != 2) return;

        double rank = Double.parseDouble(data[0]);
        String neighbors = data[1];
        String[] neighborList = neighbors.split(",");

        // Emit the current node with its rank and neighbors for reconstructing the graph
        context.write(new Text(node), new Text(rank + "|" + neighbors));

        // Distribute rank to neighbors
        int numNeighbors = neighborList.length;
        if (numNeighbors > 0) {
            double contribution = rank / numNeighbors;
            for (String neighbor : neighborList) {
                context.write(new Text(neighbor), new Text(String.valueOf(contribution)));
            }
        }
    }
}
