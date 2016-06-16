package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

/**
 * Keeps track of all statistics of query 1 and query 2
 * and writes them in the right format to the performance file, if
 * all statistics are determined.
 * @author mayercn
 *
 */
public class Statistics {

	private static double durationQ1 = -1;
	private static double durationQ2 = -1;
	private static double averageLatencyQ1 = -1;
	private static double averageLatencyQ2 = -1;
	
	/**
	 * Sets the total duration of query 1 in ms.
	 * @param durationQ1
	 */
	public synchronized static void setDurationQ1(double durationQ1) {
		Statistics.durationQ1 = durationQ1 / 1000.0;
		System.out.println(Statistics.durationQ1);
		writeIfReady();
	}
	
	/**
	 * Sets the total duration of query 2 in ms.
	 * @param durationQ2
	 */
	public synchronized static void setDurationQ2(double durationQ2) {
		Statistics.durationQ2 = durationQ2 / 1000.0;
		System.out.println(Statistics.durationQ2);
		writeIfReady();
	}
	
	/**
	 * Sets the average latency of query 1 per (out-) event in ms
	 * @param averageLatencyQ1
	 */
	public synchronized static void setAverageLatencyQ1(double averageLatencyQ1) {
		Statistics.averageLatencyQ1 = averageLatencyQ1 / 1000000000.0;
		System.out.println(Statistics.averageLatencyQ1);
		writeIfReady();
	}
	
	/**
	 * Sets the average latency of query 2 per (out-) event in ms
	 * @param averageLatencyQ2
	 */
	public synchronized static void setAverageLatencyQ2(double averageLatencyQ2) {
		Statistics.averageLatencyQ2 = averageLatencyQ2 / 1000000000.0;
		System.out.println(Statistics.averageLatencyQ2);
		writeIfReady();
	}
	
	private synchronized static void writeIfReady() {
		if (durationQ1>=0 && durationQ2>=0 && averageLatencyQ1>=0 && averageLatencyQ2>=0) {
			try {
				BufferedWriter performanceWriter = new BufferedWriter(new FileWriter(new File(Constants.PERFORMANCE_OUTFILE)));
				
				DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.ENGLISH);
				otherSymbols.setDecimalSeparator('.');
				DecimalFormat df = new DecimalFormat("#0.000000", otherSymbols);
				
				performanceWriter.write(df.format(durationQ1) + "\n"
						+ df.format(averageLatencyQ1) + "\n" 
						+ df.format(durationQ2) + "\n" 
						+ df.format(averageLatencyQ2));
				performanceWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}
