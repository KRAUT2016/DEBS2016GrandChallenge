package query2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import common.events.Comment;
import common.events.Friendship;
import common.events.Like;

public class CommentProcessor implements Comparable<CommentProcessor> {

	public CommentProcessor(Graph graph, Comment comment) {
		this.comment = comment;
		this.comment_id = comment.getComment_id();
		this.likes = new HashSet<Long>();
		this.largestRange = 0;
		this.graph = graph;
	}
	
	private Comment comment;		// comment this processor is responsible for
	private long comment_id;			// id of the comment
	private HashSet<Long> likes;	// list of vertices that liked this comment
	private int largestRange;		// size of largest like-clique for this comment
	private Graph graph;			// the graph that can be asked, whether two friendship exist
	
	public Comment getComment() {
		return comment;
	}
	
	public long getComment_id() {
		return comment_id;
	}

	public int getLargestRange() {
		return largestRange;
	}
	
	/**
	 * The range can potentially change due to this update. Returns
	 * true, if the range has changed due to the friendship update.
	 * Otherwise, false is returned.
	 * @param friendship
	 */
	public boolean onFriendship(Friendship friendship) {
		if (likes.contains(friendship.getUser_id_1()) 
				&& likes.contains(friendship.getUser_id_2())) {
			/*
			 * Both users liked the comment: largest clique might have changed.
			 * But largest clique could only be changed, if both users participate in new largest clique.
			 * Hence, we have to schedule only one vertex.
			 */
			int oldRange = largestRange;
			int temp1 = Clique.maxClique(graph, friendship.getUser_id_1(), likes, largestRange);
			if (temp1>largestRange) {
				largestRange = temp1;
			}
			if (largestRange>oldRange) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}
	

	/**
	 * A new like for this comment. This like can lead to new larger cliques.
	 * Note, that in the meantime, new friendship relations could have emerged.
	 * However, a potential new clique could have only emerged around the new like.
	 * @param like
	 */
	public void onLike(Like like) {
		likes.add(like.getUser_id());
		int temp = Clique.maxClique(graph, like.getUser_id(), likes, largestRange);
		if (temp>largestRange) {
			largestRange = temp;
		}
	}
	
	@Override
	public boolean equals(Object o) {
		if (this==o) {
			return true;
		}
		CommentProcessor p = (CommentProcessor) o;
		if (p.comment_id==comment_id) {
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return comment.getComment().hashCode();
	}
	
	@Override
	public String toString() {
		return "(id " + getComment().getComment_id() + ": " + largestRange + " " + likes.size() + " " + comment.getComment() + ")";
	}
	
	@Override
	/**
	 * Returns a negative value, if the parameter comment has smaller range.
	 * When sorting the list in ascending order, the parameter comment would be after this comment.
	 * DEBS requirement:
	 * [...] ordered by range from largest to smallest,
	 * with ties broken by lexicographical ordering (ascending)
	 * with ties broken by comment_id
	 */
	public int compareTo(CommentProcessor o) {
		if (largestRange>o.largestRange) {
			return -1;
		} else if (largestRange==o.largestRange) {
			// compare comments by lexicographical order
			String c1 = o.getComment().getComment();
			String c2 = this.getComment().getComment();
			int returnValue = c2.compareToIgnoreCase(c1);
//			int returnValue = c2.compareTo(c1);
			if (returnValue==0) {
				if (comment_id>o.comment_id) {
					return -1;
				} else if (comment_id==o.comment_id) {
					return 0;
				} else {
					return 1;
				}
			} else {
				return returnValue;
			}
		} else {
			return 1;
		}
	}
	
	public static void main(String[] args) {
		List<Long> l = new ArrayList<Long>();
		l.add(new Long(123));
		l.add(new Long(334));
		System.out.println(l.contains(123));
	}

	
	public int numberOfLikes() {
		return likes.size();
	}
}
