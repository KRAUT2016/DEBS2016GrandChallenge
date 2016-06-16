package util;

/**
 * Put constants here, e.g., Strings
 * @author mayerrn
 *
 */
public class Constants {
	
	public static final int Q1SIZE = 3;
	public static final int Q2SIZE = 3;
	
//	public static final int Q1TOP3BINSIZE = 1; // bins contain x scores range (e.g., 0-9, 10-19, etc. for 10)
	
//	public static final int SORTING_SCORE_TH = 70; 
	
	/**
	 * parsing the incoming datafiles
	 */
	public static final char VALUE_SEPARATOR = '|'; 
	public static final String Q1_OUTFILE = "q1.txt";
	public static final String Q2_OUTFILE = "q2.txt";
	public static final String PERFORMANCE_OUTFILE = "performance.txt";
	public static final String LOG_OUTFILE = "log.txt";
	
	public static final int BUFFERING_IN_READERS = 1024*1024*16;
	
	/**
	 * query 1 specifics
	 */
	public static final int Q1_NUM_WORKERS = 2; // number of worker threads in query 1
	public static final long MS_24H = 86400000; // 24 hours in milliseconds
	
	/**
	 * query 2
	 */
	public static final int Q2_NUM_WORKERS = 1; // number of worker threads in query 2
	public static int Q2_K;  // size of topK list of comments (set by main parameters)
	public static long Q2_D;	// comments that were created not more than d secs ago (set by main parameters)
	public static final boolean Q2_REPLICATE_GRAPH = true;	// replicate the friendship graph on each worker?
}
