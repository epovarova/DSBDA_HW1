import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    private final String testLog5 = "5,1,Mar 26 20:55:07,ubuntu,gnome-shell[9738]:, [AppIndicatorSupport-DEBUG] Registering StatusNotifierItem :1.62/org/ayatana/NotificationItem/livepatch";
    private final String testLog4 = "4,1,Mar 26 20:55:07,ubuntu,gnome-software[11641]:, no app for changed ubuntu-dock@ubuntu.com";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testLog5))
                .withInput(new LongWritable(), new Text(testLog4))
                .withOutput(new Text("5 Mar 26 20-21"), new IntWritable(1))
                .withOutput(new Text("4 Mar 26 20-21"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver
                .withInput(new Text(testLog5), values)
                .withOutput(new Text(testLog5), new IntWritable(2))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testLog5))
                .withInput(new LongWritable(), new Text(testLog5))
                .withOutput(new Text("5 Mar 26 20-21"), new IntWritable(2))
                .runTest();
    }
}
