import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class NYHotSpots implements Serializable {

	private static final int LATESTDAY = 30;
	private static final int EARLIESTDAY = 0;
	private static final int LONGHIGHERLIMIT = -7370;
	private static final int LONGLOWERLIMIT = -7425;
	private static final int LATHIGHERLIMIT = 4090;
	private static final int LATLOWERLIMIT = 4050;
	private int N = 55 * 40 * 31;

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setAppName("NYHotSpots").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		NYHotSpots n = new NYHotSpots();
		
		String inputFile = args[0];
		
		String outputFile = args[1];
		
		JavaPairRDD<String, Integer> counts = n.creatingCubeMap(sc, inputFile);
		
		n.neighborCalculate(counts, outputFile);
		
	}
	public JavaPairRDD<String, Integer> creatingCubeMap(JavaSparkContext sc, String filepath) throws IOException {

		JavaRDD<String> textFile = sc.textFile(filepath, 4);
		JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) {
				return Arrays.asList(s).iterator();
			}
		});

		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				if ((Double.parseDouble(s.split(",")[5]) > -74.25 && (Double.parseDouble(s.split(",")[5]) < -73.70))
						&& (Double.parseDouble(s.split(",")[6]) > 40.50
								&& (Double.parseDouble(s.split(",")[6]) < 40.90))) {
					String lon = String.valueOf((Double.parseDouble(s.split(",")[5]) - 0.01) * 100).substring(0,
							5);
					String lat = String.valueOf(Double.parseDouble(s.split(",")[6]) * 100).substring(0, 4);
					String day = String.format("%02d",
							Integer.parseInt(s.split(",")[1].split("-")[2].substring(0, 2)) - 1);
					return new Tuple2<String, Integer>(lat + lon + day, 1);
				} else {
					return new Tuple2<String, Integer>("IGNORE_KEY", 1);
				}
			}
		});

		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});

		counts = counts.filter(new Function<Tuple2<String, Integer>, Boolean>() {

			@Override
			public Boolean call(Tuple2<String, Integer> paramT1) throws Exception {
				return !paramT1._1.equalsIgnoreCase("IGNORE_KEY");
			}
		});
		
		return counts;

	}

	public void neighborCalculate(JavaPairRDD<String, Integer> jpRDD, String outputFile) throws IOException {

		Map<String, Integer> cellMap = jpRDD.collectAsMap();
		
		int cell_x = 0, start_x = 0, end_x = 0;
		int cell_y = 0, start_y = 0, end_y = 0;
		int cell_z = 0, start_z = 0, end_z = 0;

		double Wij = 0;
		double sumXj = 0;
		double numerator = 0, denominator = 0, Gstar = 0;
		double Xbar = calculateMean(cellMap);

		double stdDeviationS = calcStdDeviation(cellMap);

		HashMap<String, Double> HotSpotG = new HashMap<String, Double>();

		Iterator<Entry<String, Integer>> it = cellMap.entrySet().iterator(); // Iterate
																				// through
																				// map
		while (it.hasNext()) {
			Entry<String, Integer> pair = it.next();
			String coordinates = pair.getKey().toString();

			cell_x = Integer.parseInt(coordinates.substring(0, 4)); // latitude
			cell_y = Integer.parseInt(coordinates.substring(4, 9)); // longitude
																			// //Review
																			// =>
																			// Input
																			// of
																			// longitude
																			// without
																			// -ve
																			// sign.
																			// Hence
																			// multiplied
																			// by
																			// -1
			cell_z = Integer.parseInt(coordinates.substring(9)); // date

			ArrayList<Integer> XjList = new ArrayList<>();

			start_x = cell_x - 1;
			end_x = cell_x + 1;

			start_y = cell_y - 1;
			end_y = cell_y + 1;

			start_z = cell_z - 1;
			end_z = cell_z + 1;

			if (start_x < LATLOWERLIMIT)
				start_x = cell_x;
			if (end_x > LATHIGHERLIMIT)
				end_x = cell_x;

			if (start_y < LONGLOWERLIMIT)
				start_y = cell_y;
			if (end_y > LONGHIGHERLIMIT)
				end_y = cell_y;

			if (start_z < EARLIESTDAY)
				start_z = cell_z;
			if (end_z > LATESTDAY)
				end_z = cell_z;

			Wij = 0;

			for (int x = start_x; x <= end_x; x++) {
				for (int y = start_y; y <= end_y; y++) {
					for (int z = start_z; z <= end_z; z++) {

						String key = Integer.toString(x) + (Integer.toString(y)) + String.format("%02d", z); // neighbor
																												// cells
						if (cellMap.containsKey(key)) {
							Integer value = cellMap.get(key);
							XjList.add(value);
						}

						Wij += 1;
					}
				}
			}

			// Calculate G* for every tuple in the JavaPairRDD/Map.
			// Numerator calculation

			numerator = 0;
			sumXj = 0;
			for (int i = 0; i < XjList.size(); i++) {
				sumXj += XjList.get(i);
			}

			numerator = sumXj - Xbar * Wij;
			// summation of Wij => valid values 8, 12, 18, 27

			// Denominator Calculation
			denominator = Math.sqrt(Wij * (N - Wij) / (N - 1)) * stdDeviationS; // Denominator
																				// Change
																				// Suhasini#1

			Gstar = numerator / denominator;

			HotSpotG.put(coordinates, Gstar); // Final G* for every tuple

		}

		// System.out.println(HotSpotG);
		Comparator<String> comparator = new ValueComparator(HotSpotG);
		// TreeMap is a map sorted by its keys.
		// The comparator is used to sort the TreeMap by keys.
		TreeMap<String, Double> result = new TreeMap<String, Double>(comparator);
		result.putAll(HotSpotG);

		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
		int count = 1;
		for(String key : result.keySet()) {
			if(count == 51) {
				break;
			}
			String lat = key.substring(0, 4);
			String lon = key.substring(4, 9);
			String day = String.valueOf(Integer.parseInt(key.substring(9)));
			String gstar = Double.toString(new Double(result.get(key)));
			bw.write(lat + "," + lon + "," + day + "," + gstar);
			bw.newLine();
			count++;
		}
		bw.flush();
		bw.close();
		
		
	}

	// Xbar calculation

	private double calculateMean(Map<String, Integer> cMap) {
		double sum = 0;

		Iterator<Entry<String, Integer>> it = cMap.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Integer> pair = it.next();

			sum += cMap.get(pair.getKey());

		}

		double meanXbar = sum / N;
		// System.out.println("calculateMean output : "+meanXbar);
		return meanXbar;

	}

	// S calculation => standard deviation

	private double calcStdDeviation(Map<String, Integer> cMap) {
		double sum = 0;

		Iterator<Entry<String, Integer>> it = cMap.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Integer> pair = it.next();
			double value = cMap.get(pair.getKey());
			sum += (value * value);

			// System.out.println(pair.getKey() + " = " + pair.getValue());
		}

		double divProd = sum / N;

		double mean = calculateMean(cMap);
		//System.out.println("Mean : "+mean);
		double stdDeviation = Math.sqrt(divProd - (mean * mean));
		 //System.out.println("Standard dev: "+stdDeviation);
		return stdDeviation;

	}

}