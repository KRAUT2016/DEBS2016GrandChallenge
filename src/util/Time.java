package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Random;
import java.util.TimeZone;

public class Time {
	
	private SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
	private Calendar cal;
	private static final long millisecsDay = 24 * 3600000;
	private static final long millisecsLeapYear = 366 * millisecsDay;
	private static final long millisecsNonLeapYear = 365 * millisecsDay;
//	private static final long millisecs
//	private static int latestYear = 1970;
//	private static long millisecondsPassedSince1970;
	private HashMap<Integer,Long> millisecsPassedSince1970_cache;
	private HashMap<Integer,Integer> month_to_totalDays_cache_leap;
	private HashMap<Integer,Integer> month_to_totalDays_cache;
	private HashMap<Integer,Boolean> isLeap_cache;

	public Time(){
		dateformat.setTimeZone(TimeZone.getTimeZone("UTC"));
		cal = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));
		millisecsPassedSince1970_cache = new HashMap<Integer,Long>();
		month_to_totalDays_cache = new HashMap<Integer, Integer>();
		month_to_totalDays_cache_leap = new HashMap<Integer, Integer>();
		isLeap_cache = new HashMap<Integer, Boolean>();
//		millisecondsPassedSince1970 = millisecsPassedSince1970(2000);
//		latestYear = 2000;
	}
	
	/**
	 * Converts s (see below for an example) to the milliseconds
	 * that have passed since Unix timestamp
	 * @param s (e.g. 2010-02-05T18:37:22.634+0000)
	 * @return
	 * @throws ParseException 
	 */
	public long timeStamp_slow(String s) throws ParseException {
		long ts = dateformat.parse(s).getTime();
		return ts;
	}
	
	//	/**
	//	 * Converts the number of milliseconds passed since Unix timestamp
	//	 * to the string representation (e.g. 2010-02-05T18:37:22.634+0000)
	//	 * @param time
	//	 * @return
	//	 */
	//	public String timeStamp(long time) {
	//		return timeStamp2(time);
	//	}
		
		/**
		 * Converts s (see below for an example) to the milliseconds
		 * that have passed since Unix timestamp
		 * @param s (e.g. 	2010-02-05T18:37:22.634+0000)
		 * 			pos:	01234567890123456789012345678
		 * @return
		 * @throws ParseException 
		 */
		public long timeStamp2(String s) throws ParseException {
			char[] s1 = s.toCharArray();
			int year = Integer.valueOf(new String(s1,0,4));
			int month = Integer.valueOf(new String(s1,5,2))-1;	// months are counted from 0 to 11
			int date = Integer.valueOf(new String(s1,8,2));	// days are counted from 0 to ...
			int hourOfDay = Integer.valueOf(new String(s1,11,2));
			int minute = Integer.valueOf(new String(s1,14,2));
			int second = Integer.valueOf(new String(s1,17,2));
			int milliseconds = Integer.valueOf(new String(s1,20,3));
	//		System.out.println(year + " " + month + " " + date + " " + hourOfDay + " " + minute + " " + second + " " + milliseconds);
			
			cal.clear();
			cal.set(year, month, date, hourOfDay, minute, second);
			cal.set(Calendar.MILLISECOND, milliseconds);
	//		System.out.println("Instant: " + cal.toInstant());
			
			return cal.getTimeInMillis();
		}
	/**
	 * Converts s (see below for an example) to the milliseconds
	 * that have passed since Unix timestamp
	 * @param s (e.g. 	2010-02-05T18:37:22.634+0000)
	 * 			pos:	01234567890123456789012345678
	 * @return
	 */
	public long timeStamp(String s) {
		char[] s1 = s.toCharArray();
		int year = Integer.valueOf(new String(s1,0,4));
		int month = Integer.valueOf(new String(s1,5,2))-1;	// months are counted from 0 to 11
		int date = Integer.valueOf(new String(s1,8,2))-1;
		int hourOfDay = Integer.valueOf(new String(s1,11,2));
		int minute = Integer.valueOf(new String(s1,14,2));
		int second = Integer.valueOf(new String(s1,17,2));
		int milliseconds = Integer.valueOf(new String(s1,20,3));
		
		long totalMillisecs = milliseconds;
		totalMillisecs += second * 1000;
		totalMillisecs += minute * 60000;
		totalMillisecs += hourOfDay * 3600000;
		totalMillisecs += date * millisecsDay;
		totalMillisecs += daysFullMonths(year, month) * millisecsDay;
		totalMillisecs += millisecsPassedSince1970(year);
//		return totalMillisecs - 86400000;
		return totalMillisecs;
	}
	
	/**
	 * Returns the number of days in the year until
	 * excluding month.
	 * @param year
	 * @param month
	 * @return
	 */
	private int daysFullMonths(int year, int month) {
		boolean isLeap = isLeap(year);
		
		// check whether value is cached already
		if (isLeap) {
			Integer x = month_to_totalDays_cache_leap.get(month);
			if (x!=null) {
				return x;
			}
		}
		if (!isLeap) {
			Integer x = month_to_totalDays_cache.get(month);
			if (x!=null) {
				return x;
			}
		}
		
		// value was not cached
		int dayCount = 0;
		for (int i=0; i<11; i++) {
			if (i>=month) {
				// do not count days of current month
				break;
			} else {
				int days;
				if (i==3 || i==5 || i==8 || i==10) {
					days = 30;
				} else if (i==1) {
					if (isLeap) {
						days = 29;
					} else {
						days = 28;
					}
				} else {
					days = 31;
				}
				dayCount += days;
			}
		}
		if (isLeap) {
			month_to_totalDays_cache_leap.put(month, dayCount);
//			System.out.println(month_to_totalDays_cache_leap);
		} else {
			month_to_totalDays_cache.put(month, dayCount);
//			System.out.println(month_to_totalDays_cache);
		}
		return dayCount;
	}

	/**
	 * Returns true, if year is a leap year
	 * @param year
	 * @return
	 */
	private boolean isLeap(int year) {
		Boolean isLeap = isLeap_cache.get(year);
		if (isLeap != null) {
			return isLeap;
		}
		if (year%4!=0) {
			isLeap = false;
		} else {
			if (year%100==0) {
				if (year%400==0) {
					isLeap = true;
				} else {
					isLeap = false;
				}
			} else {
				isLeap = true;
			}
		}
		isLeap_cache.put(year, isLeap);
		return isLeap;
	}
	
	/**
	 * Returns the number of milliseconds passed since 2000
	 * @param year
	 * @return
	 */
	private long millisecsPassedSince1970(int year) {
		Long cached = millisecsPassedSince1970_cache.get(year);
		if (cached != null) {
			// cache hit
			return cached;
		}

		long passed = 0;
		for (int i=1970; i<year; i++) {
			if (isLeap(i)) {
				passed += millisecsLeapYear;
			} else {
				passed += millisecsNonLeapYear;
			}
		}
		millisecsPassedSince1970_cache.put(year, passed);
//		System.out.println(millisecsPassedSince1970_cache);
		return passed;
	}
	
/**
	 * Converts the number of milliseconds passed since Unix timestamp
	 * to the string representation (e.g. 2010-02-05T18:37:22.634+0000)
	 * @param time
	 * @return
	 */
	public String timeStamp(long time) {
		String s = dateformat.format(new Date(time));
		return s;
	}
	
	public static void main(String[] args) throws ParseException {
		String s = "2010-12-23T18:19:57.234+0000";
//		String s = "1975-01-02T00:00:00.000+0000";
		Time time = new Time();
		System.out.println(s);
		System.out.println(time.timeStamp(s) + " =\n" + time.timeStamp2(s));
		System.out.println(time.timeStamp(time.timeStamp(s)));
		long startTime = System.currentTimeMillis();
		for (int i=0; i<10000000; i++) {
//		while (true) {
			long timestamp1 = time.timeStamp(s);
		}
		long duration = System.currentTimeMillis() - startTime;
		System.out.println("Duration " + duration);
		
//		
//		
//		
//		
//		for (int i=0; i<10000; i++) {
//			time.timeStamp(s);
//		}
//		long duration = System.currentTimeMillis() - startTime;
//		System.out.println("Duration " + duration);
//		startTime = System.currentTimeMillis();
//		for (int i=0; i<10000; i++) {
//			time.timeStamp2(s);
//		}
//		duration = System.currentTimeMillis() - startTime;
//		System.out.println("Duration " + duration);
//		System.out.println(time.timeStamp(s));
//		System.out.println(time.timeStamp2(s));
		
//		long startTime = System.currentTimeMillis();
//		for (int i=0; i<100000; i++) {			
//			long fd = time.timeStamp(s);
//		}
//		System.out.println("Total time: " + (System.currentTimeMillis()-startTime));
//		System.out.println(s.subSequence(0,23));
//		
//		startTime = System.currentTimeMillis();
//		for (int i=0; i<100000; i++) {			
//			long fd = time.timeStamp_slow(s);
//		}
//		System.out.println("Total time: " + (System.currentTimeMillis()-startTime));
		
//		// testing
//		for (int i=0; i<10000; i++) {
//			Random random = new Random();
//			long ts = random.nextLong();
//			ts = Math.max(1293128397234L, ts);
//			String tts = time.timeStamp(ts);
//			tts = tts.substring(tts.length()-28);
//			System.out.println();
//			System.out.println("ts: " + ts);
//			System.out.println("form: " + tts);
//			if (time.timeStamp_slow(tts)!=time.timeStamp(tts)) {
//				System.err.println("Error: " + ts + " " + time.timeStamp(tts));
//			}
////			if (ts!=time.timeStamp_slow(tts)) {
////				System.err.println("Error: " + ts + " " + time.timeStamp_slow(tts));
////			}
//		}
	}
}
