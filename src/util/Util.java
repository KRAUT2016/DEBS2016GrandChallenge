package util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import query2.CommentProcessor;
import common.events.Comment;

public class Util {

	
	public static String[] fastSplit2(String s, int splitSize, char delimitter) {
		StringTokenizer tokenizer = new StringTokenizer(s,"\\|");
		String[] result = new String[tokenizer.countTokens()];
		for (int i=0; i<result.length; i++) {
			result[i] = tokenizer.nextToken("\\|");
		}
		return result;
	}
	
	/**
	 * a faster splitting implementation 
	 * @param s string to be split
	 * @return
	 */
	public static String[] fastSplit(String s, int splitSize, char delimitter) {
		
		String[] result = new String[splitSize];
		int a = -1;
		int b = 0;

		for (int i = 0; i < splitSize; i++) {

			while (b < s.length() && s.charAt(b) != delimitter)
				b++;
			result[i] = s.substring(a+1, b);
			a = b;
			b++;		
		}

		return result;
	}
	
	
	public static String[] fastCommentSplit(String s, int splitSize, char delimitter){
		String[] result = new String[splitSize];
		int a = -1;
		int b = 0;

		for (int i = 0; i < splitSize; i++) {

			while (b < s.length() && s.charAt(b) != delimitter)
				b++;
			if (i != 2){
				result[i] = s.substring(a+1, b);
			}
			a = b;
			b++;		
		}

		return result;
	}
	
	
	public static String[] fastPostSplit(String s, int splitSize, char delimitter){
		String[] result = new String[splitSize];
		int a = -1;
		int b = 0;

		for (int i = 0; i < splitSize; i++) {

			while (b < s.length() && s.charAt(b) != delimitter)
				b++;
			if (i != 2){
				result[i] = s.substring(a+1, b);
			}
			a = b;
			b++;		
		}

		return result;
	}
	
	/**
	 * Hardcoded fast splitting implementation 
	 * @param s string to be split
	 * @return
	 */
	public static String[] fastSplitxxx(String s, int splitSize, char delimitter) {
		int length = s.length();
		String[] result = new String[splitSize];
		int count = 0;
		int index = 0;
		char[] current = new char[length];
		for (int i = 0; i < length; i++) {
			char c = s.charAt(i);
			if (c==delimitter) {
				result[index] = new String(current,0,count);
				count = 0;
				index++;
				if (index==splitSize) {
					return result;
				}
			} else {
				current[count]=c;
				count++;
			}
		}
		result[index] = new String(current,0,count);
		return result;
	}
	
	/**
	 * Inserts o into l, s.t. the list l remains sorted. Note that the objects have to
	 * implement the comparable interface and should be comparable to each others.
	 * @param l
	 * @param o
	 */
	public static void sortedInsert(List<CommentProcessor> l, CommentProcessor o) {
		int pos = 0;
		for (int i=l.size()-1; i>=0; i--) {
			CommentProcessor el = l.get(i);
			if (el.compareTo(o)<=0) {
				// el has greater range; insert here
				pos = i+1;
				break;
			}
		}
		l.add(pos, o);
//		l.add(o);
//		Collections.sort(l);
	}
	
	/**
	 * Returns the element with largest range with ties broken by
	 * lexicographical order. Note that elements in the exclude list
	 * are no candidates for the max element.
	 * @param l
	 * @return
	 */
	public static CommentProcessor getMax(Collection<CommentProcessor> l, List<CommentProcessor> exclude) {
		CommentProcessor max = null;
		for (CommentProcessor p : l) {
			if (max==null) {
				if (!exclude.contains(p)) {
					// p has higher score
					max = p;
				}
			} else {
				if (!exclude.contains(p)) {
					if (p.getLargestRange()>max.getLargestRange()) {
						// p has higher score
						max = p;
					} else if (p.getLargestRange()==max.getLargestRange() && p.compareTo(max)<0) {
						// p has higher score
						max = p;
					}
				}
			}
			
//			if (!exclude.contains(p) && (max==null || p.compareTo(max)<0)) {
//				// p has higher score
//				max = p;
//			}
		}
		return max;
	}
	
	/**
	 * Copies a set of longs.
	 * @param l
	 * @return
	 */
	public static Set<Long> copy(Set<Long> l) {
		Set<Long> l2 = new HashSet<Long>();
		for (Long lo : l) {
			l2.add(lo);
		}
		return l2;
	}
	
	
	/**
	 * Returns true, if the two lists of comment processors equal each other.
	 * Two comment processors are equal, if their comment_ids are equal or if their comments are equal.
	 * "The k strings and the corresponding timestamp must be printed only when some input triggers a change of the output"
	 * Such a change is only happening, if a comment has a different position in the topK list.
	 * 
	 * @param topK
	 * @param oldTopK
	 * @return
	 */
	public static boolean listEquals(List<CommentProcessor> l1, List<CommentProcessor> l2) {
		if (l1.size()!=l2.size()) {
			return false;
		}
		for (int i=0; i<l1.size(); i++) {
//			if (l1.get(i).compareTo(l2.get(i))!=0) {
			CommentProcessor a = l1.get(i);
			CommentProcessor b = l2.get(i);
			if (a.getComment_id()!=b.getComment_id()) {
				if (!a.getComment().getComment().equals(b.getComment().getComment())) {
					// comment_id and comment is different: list has changed
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * Returns a copy of the list (flat).
	 * @param topK
	 * @return
	 */
	public static List<CommentProcessor> copy(List<CommentProcessor> topK) {
		List<CommentProcessor> copy = new ArrayList<CommentProcessor>();
		for (CommentProcessor p : topK) {
			copy.add(p);
		}
		return copy;
	}


	public static void main(String[] args) {
		
		String test = "2010-09-05T19:46:33.283+0000|25770464175|4139|About 50 Cent, Billboard ranked his al. About Charge It 2 da Game, of America.|Baruch Dego|25770464173|";
		long time1 = System.currentTimeMillis();
		for (int i=0; i<10000; i++) {
			Util.fastSplit2(test, 7, '|');
		}
		long duration = System.currentTimeMillis() - time1;
		time1 = System.currentTimeMillis();
		for (int i=0; i<10000; i++) {
			Util.fastSplit(test, 7, '|');
		}
		long duration2 = System.currentTimeMillis() - time1;
		System.out.println("Duration 1 : " + duration);
		System.out.println("Duration 2 : " + duration2);
		
		
		for (int i=0; i<6; i++) {
			System.out.println(test.split("\\|")[i]);
			System.out.println(Util.fastSplit(test, 7, '|')[i]);
			System.out.println();
		}
		
		char[] cars = new char[1];
		cars[0]='a';
		System.out.println("Trommelwirbel: " + new String(cars,0,0));
		HashSet<Long> s1 = new HashSet<Long>();
		s1.add(123L);
		s1.add(124L);
		
		HashSet<Long> s2 = new HashSet<Long>();
		s2.add(124L);
		s2.add(123L);
		
		System.out.println(s1.equals(s2));
	}
	
}
