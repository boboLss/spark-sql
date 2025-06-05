package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;

public class AppIncidents {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Incidents Par Service").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("file:///opt/data/incidents.csv");

        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> !line.equals(header));

        JavaPairRDD<String, Integer> incidentsParService = data
                .mapToPair(line -> {
                    String[] parts = line.split(",");
                    String service = parts[3].trim();
                    return new Tuple2<>(service, 1);
                })
                .reduceByKey(Integer::sum);

        System.out.println("=== Nombre d’incidents par service ===");
        incidentsParService.collect().forEach(System.out::println);

        JavaPairRDD<String, Integer> incidentsParAnnee = data
                .mapToPair(line -> {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                    String[] parts = line.split(",");
                    String dateStr = parts[4].trim();
                    String year = LocalDate.parse(dateStr, formatter).getYear() + "";
                    return new Tuple2<>(year, 1);
                })
                .reduceByKey(Integer::sum);

        Comparator<Tuple2<String, Integer>> comparator = new SerializableComparator();

        List<Tuple2<String, Integer>> top2Annees = incidentsParAnnee
                .takeOrdered(2, comparator);

        System.out.println("=== Les deux années avec le plus d’incidents ===");
        top2Annees.forEach(System.out::println);
        sc.close();
    }

    public static class SerializableComparator implements Comparator<Tuple2<String, Integer>>, java.io.Serializable {
        @Override
        public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return t2._2.compareTo(t1._2);
        }
    }
}