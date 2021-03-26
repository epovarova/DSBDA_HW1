package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Маппер: проставляет единицы по ключам равным уровню логирования в строке
 */
public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // пользуем одну writable сущность
    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();
    Pattern dateAndHourPattern = Pattern.compile("((Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) [0-9]{1,2}) ([0-9]{1,2})");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // регулярное выражение, используемое для определения подходящего формата лога
        String checkValidLogFormatRegex = "[0-7],[0-9]{1,2},(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) [0-9]{1,2} [0-9]{2}:[0-9]{2}:[0-9]{2},.*?,.*?,.*";
        if (Pattern.matches(checkValidLogFormatRegex, line)) {
            // строка лога начинается с цифрового обозначения уровня лога
            String logLevel = line.substring(0, 1);
            Matcher dateAndHour = dateAndHourPattern.matcher(line);
            dateAndHour.find();
            // дата, когда произошло логируемое событие
            String logDate = dateAndHour.group(1);
            // час, когда произошло логируемое событие
            int logHour = Integer.parseInt(dateAndHour.group(3));
            word.set(String.format("%s %s %d-%d", logLevel, logDate, logHour, logHour+1));
            context.write(word, one);
        } else {
            // если формат лога иной, то увеличиваем счетчик
            context.getCounter(CounterType.MALFORMED).increment(1);
        }
    }
}
