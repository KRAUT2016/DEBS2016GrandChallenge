package query2_old;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Util {

	/**
	 * Inserts o into l, s.t. the list l remains sorted. Note that the objects have to
	 * implement the comparable interface and should be comparable to each others.
	 * TODO: use bubble sort to get O(n) performance.
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
//		HashSet<CommentProcessor> exclude = new HashSet<CommentProcessor>(exclude2);
		CommentProcessor max = null;
		for (CommentProcessor p : l) {
			if ((max==null || p.compareTo(max)<0) && !exclude.contains(p)) {
				// p has higher score
				max = p;
			}
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
	 * Two comment processors are equal, if their comment_ids, comments AND ranges are equal!!
	 * @param topK
	 * @param oldTopK
	 * @return
	 */
	public static boolean listEquals(List<CommentProcessor> l1, List<CommentProcessor> l2) {
		if (l1.size()!=l2.size()) {
			return false;
		}
		for (int i=0; i<l1.size(); i++) {
			if (l1.get(i).compareTo(l2.get(i))!=0) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Returns true, if the list consists of similar comments using only comment_id as comparator.
	 * @param lastTopK
	 * @param topK
	 * @return
	 */
	public static boolean listEqualCommentIDs(List<CommentProcessor> l1,
			List<CommentProcessor> l2) {
		if (l1.size()!=l2.size()) {
			return false;
		}
		for (int i=0; i<l1.size(); i++) {
			if (l1.get(i).getComment().getComment_id()!=l2.get(i).getComment().getComment_id()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Returns a copy of the list (deep).
	 * @param topK
	 * @return
	 */
	public static List<CommentProcessor> copyCommentProcessorsIgnoreLikes(List<CommentProcessor> topK) {
		List<CommentProcessor> copy = new ArrayList<CommentProcessor>();
		for (CommentProcessor p : topK) {
			copy.add(p.copyIgnoreLikes());
		}
		return copy;
	}


	public static void main(String[] args) {
		HashSet<Long> s1 = new HashSet<Long>();
		s1.add(123L);
		s1.add(124L);
		
		HashSet<Long> s2 = new HashSet<Long>();
		s2.add(124L);
		s2.add(123L);
		
		System.out.println(s1.equals(s2));
	}
	
}
