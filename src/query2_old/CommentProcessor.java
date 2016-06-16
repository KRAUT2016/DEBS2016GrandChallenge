package query2_old;

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
		this.likes = new HashSet<Long>();
		this.largestRange = 0;
		this.graph = graph;
	}
	
	public CommentProcessor(Graph graph, Comment comment, int largestRange) {
		this.comment = comment;
		this.likes = new HashSet<Long>();
		this.largestRange = largestRange;
		this.graph = graph;
	}
	
	private Comment comment;		// comment this processor is responsible for
	private HashSet<Long> likes;	// list of vertices that liked this comment
	private int largestRange;		// size of largest like-clique for this comment
	private Graph graph;			// the graph that can be asked, whether two friendship exist
	
	public Comment getComment() {
		return comment;
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
			 * Both users liked the comment: largest clique might have changed
			 */
			int oldRange = largestRange;
			updateLargestRange(friendship.getUser_id_1());
			updateLargestRange(friendship.getUser_id_2());
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
	 * Note, that in the meantime, the friendship relations could have changed.
	 * @param like
	 */
	public void onLike(Like like) {
		likes.add(like.getUser_id());
		updateLargestRange(like.getUser_id());
//		updateLargestRange();
	}
	
	/**
	 * Calculates the largest clique of friends in this comment.
	 * Looks only into the neighborhood of the affected user user_id.
	 * The reasoning is that the update coming from a friendship or a like
	 * can only change the largest clique in regions that change due to the update.
	 * The unchanged graph regions remain unchanged.
	 * @param user_id
	 */
	private void updateLargestRange(long user_id) {
		
		// cache getLocalDegree for this method (we can not store the cache 
		HashMap<Long,Integer> cacheDegree = new HashMap<Long, Integer>();
		
		// only if the vertex has higher degree than the largest range, it has a chance to be in the largest clique
		if (getLocalDegree(user_id, cacheDegree)<=largestRange) {
			return;
		}


		// determine candidates for largest clique,
		// i.e., all neighbors with higher degree
		Set<Long> candidates = Util.copy(likes);
		Iterator<Long> iter = candidates.iterator();
		while (iter.hasNext()) {
			Long candidate = iter.next();
			if (getLocalDegree(candidate, cacheDegree)<=largestRange) {
				iter.remove();
			}
		}
		
		// only if there are enough candidates, the largest range could change
		if (candidates.size()<=largestRange) {
			return;
		}
//		System.out.println("candidates: " + candidates);
		
		// calculate the neighbors of user_id that are in the likes
		// all possible cliques in which user_id may participate have to have one of those vertices already
		Set<Long> neighbors = new HashSet<Long>(graph.getNeighbors(user_id));
		neighbors.retainAll(likes);

		// build all cliques of size 1
		List<Set<Long>> oldCliques = new ArrayList<Set<Long>>();
		for (Long like : neighbors) {
			Set<Long> clique = new HashSet<Long>();
			clique.add(like);
			oldCliques.add(clique);
		}
		
		// build all cliques of size k>1
		for (int k=2; k<candidates.size(); k++) {
			List<Set<Long>> newCliques = new ArrayList<Set<Long>>();
			for (Long candidate : candidates) {
				// check for all cliques whether adding candidate results in a larger clique
				for (Set<Long> oldClique : oldCliques) {
					if (oldClique.contains(candidate)) {
						continue;
					}
					boolean add = true;
					for (Long cliqueVertex : oldClique) {
						if (getLocalDegree(cliqueVertex, cacheDegree)<=largestRange || 
								!graph.existsEdge(cliqueVertex, candidate)) {
							add = false;
							break;
						}
					}
					if (add) {
						Set<Long> newClique = Util.copy(oldClique);
						newClique.add(candidate);
						if (!newCliques.contains(newClique)) {
							newCliques.add(newClique);
						}
						if (k>largestRange) {
							largestRange = k;
						}
					}
				}
			}
			if (newCliques.size()==0) {
				break;
			} else {
				oldCliques = newCliques;
//				System.out.println("new cliques " + k + ": " + newCliques);
			}
		}
		

	}
	
	
	
	/**
	 * Returns the degree of this vertex w.r.t. all local likes.
	 * @param user_id
	 * @return
	 */
	private int getLocalDegree(long user_id, HashMap<Long, Integer> cacheDegree) {
		Integer cachedDegree = cacheDegree.get(new Long(user_id));
		if (cachedDegree!=null) {
			return cachedDegree;
		} else {
			int count = 0;
			for (Long like : likes) {
				if (graph.existsEdge(user_id, like)) {
					count++;
				}
			}
			cacheDegree.put(user_id, count);
			return count;
		}
	}

//	/**
//	 * Calculates the size of the largest clique of
//	 * friends liking this comment.
//	 */
//	private void updateLargestRange() {
////		System.out.println("Start updating largest like...");
//		// determine all possible cliques by size
//		HashMap<Integer, List<List<Long>>> cliques = 
//				new HashMap<Integer, List<List<Long>>>(); // a map of clique sizes to cliques
//
//		// add all cliques with size 1
//		for (Long v_id : likes) {
//			List<Long> clique = new ArrayList<Long>();
//			clique.add(v_id);
//			if (!cliques.containsKey(1)) {
//				cliques.put(1, new ArrayList<List<Long>>());
//			}
//			cliques.get(1).add(clique);
//		}
//		largestRange = 1;
//		//				System.out.println(cliques);
//		for (int k=2; k<=likes.size(); k++) {
//
//			// try to add each liking user to each existing clique to get larger clique
////			System.out.println("K=" + k + ", #cliques for last k: " + cliques.get(k-1).size());
//			for (List<Long> clique : cliques.get(k-1)) {
//				for (Long u : likes) {
//					// add user u to clique, if possible
//					if (!clique.contains(u)) {
//						boolean canBeAdded = true;
//						for (Long cliqueUser : clique) {
//							if (!graph.existsEdge(u, cliqueUser)) {
//								canBeAdded = false;
//								break;
//							}
//						}
//						if (canBeAdded) {
//							List<Long> newClique = new ArrayList<Long>(Util.copy(new HashSet<Long>(clique)));
//							newClique.add(u);
//							if (!cliques.containsKey(k)) {
//								cliques.put(k,new ArrayList<List<Long>>());
//							}
//							cliques.get(k).add(newClique);
//						}
//					}
//				}
//			}
//			if (!cliques.containsKey(k)) {
//				largestRange = k-1;
//				break;
//			} else {
//				largestRange = k;
//				cliques.remove(k-1);
//			}
//		}
////		System.out.println("... Stop updating largest range...");
//	}
	
	public CommentProcessor copyIgnoreLikes() {
		CommentProcessor p = new CommentProcessor(null, comment, largestRange);
		return p;
	}
	
	@Override
	public boolean equals(Object o) {
		CommentProcessor p = (CommentProcessor) o;
		if (p.getComment().getComment_id()==this.getComment().getComment_id()) {
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return (int)this.getComment().getComment_id() * 4;
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
		if (this.getLargestRange()>o.getLargestRange()) {
			return -1;
		} else if (this.getLargestRange()==o.getLargestRange()) {
			// compare comments by lexicographical order
			String c1 = o.getComment().getComment();
			String c2 = this.getComment().getComment();
			int returnValue = c2.compareToIgnoreCase(c1);
			if (returnValue==0) {
				if (this.getComment().getComment_id()>o.getComment().getComment_id()) {
					return -1;
				} else if (this.getComment().getComment_id()==o.getComment().getComment_id()) {
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
}
