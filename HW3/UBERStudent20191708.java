import scala.Tuple2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.io.IOException;
import java.util.*;
import java.text.*;

public final class UBERStudent20191708 {
	
	public static String returnDay(String date) {
                SimpleDateFormat format = new SimpleDateFormat("mm/dd/yyyy");
                String[] days = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
                Calendar cal = Calendar.getInstance();
                Date getDate;

                try {
                        getDate = format.parse(date);
                        cal.setTime(getDate);
                        int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
                        return days[w];
                } catch (ParseException e) {
                        e.printStackTrace();
                } catch (Exception e) {
                        e.printStackTrace();
                }
                return 0;
        }

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: UBERStudent20191708 <in-file> <out-file>");
			System.exit(1);
		}

		SparkSession spark = SparkSession
			.builder()
			.appName("UBERStudent20191708")
			.getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

		PairFunction<String, String, String> pf = new PairFunction<String, String, String>() {
			public Tuple2<String,String> call(String s) {
				String[] str = s.split(",");
				String v1 = str[0] + "," + returnDay(str[1]);
				String v2 =  str[3] + "," + str[2];
				return new Tuple2(v1, v2);
			}
		};
		JavaPairRDD<String, String> str1 = lines.mapToPair(pf);
		
		Function2<String, String, String> f2 = new Function2<String, String, String>() {
			public String call(String x, String y) {
				String[] w1 = x.split(",");
				String[] w2 = y.split(",");
				int num1 = Integer.parseInt(w1[0]) + Integer.parseInt(w2[0]);
				int num2 = Integer.parseInt(w1[1]) + Integer.parseInt(w2[1]);
				String result = num1 + "," + num2;
				return result;
			}
		};

		JavaPairRDD<String, String> counts = str1.reduceByKey(f2);

		counts.saveAsTextFile(args[1]);
		spark.stop();
	}
}
