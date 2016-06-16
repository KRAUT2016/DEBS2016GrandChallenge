package query2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import util.Util;

public class CliqueIterative {

	private static Graph graph;
	private static Set<Long> largestClique;
	
//	public static int maxCliqueIterative(Graph graph, long v_id, HashSet<Long> likes, int minClique) {
//		List<ProblemInstance> stack = new ArrayList<ProblemInstance>();
//		ProblemInstance initial = CliqueIterative.new ProblemInstance();
//		initial.clique = new HashSet<Long>();
//		initial.clique.add(v_id);
//		initial.candidates = graph.getNeighbors(v_id);
//		stack.add(initial);
//		largestClique = initial.clique;
//		while (stack.size()>0) {
//			ProblemInstance instance = stack.remove(0);
//			List<ProblemInstance> problems = process(instance);
//			stack.addAll(problems);
//		}
//		return largestClique.size();
//	}
//	
//	private static List<ProblemInstance> process(ProblemInstance instance) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//

//
//ProblemInstance:
//	Set<Long> clique;
//	Set<Long> candidates;


	/**
	 * This strategy finds the largest clique containing v_id.
	 * @param graph
	 * @param v_id
	 * @return
	 */
	public static int maxClique(Graph graph, long v_id, HashSet<Long> likes, int minClique) {
//		System.out.println("----------Start max clique-------------");
//		System.out.println(likes + " " + minClique + " " + v_id);
		CliqueIterative.graph = graph;
		
		/*
		 * We check only all cliques containing vertex v_id
		 */
		HashSet<Long> clique = new HashSet<Long>();
		clique.add(v_id);
		
		/*
		 * Candidates are only those vertices that are neighbors of v_id and
		 * are in the likes list (only likes are in the clique).
		 */
		HashSet<Long> candidates = new HashSet<Long>();
		candidates.addAll(graph.getNeighbors(v_id));
		candidates.retainAll(likes);
		
		/*
		 * Prune candidates: vertices with smaller degree (in the likes set)
		 * than minClique can never be in the max clique
		 */
		Iterator<Long> iter = candidates.iterator();
		while (iter.hasNext()) {
			Long candidate = iter.next();
			if (getLocalDegree(candidate, likes)<minClique) {
				iter.remove();
			}
		}
		
		int maxClique = maxClique(clique, candidates, minClique);
//		System.out.println("----------Max clique: " + maxClique);
		return maxClique;
	}
	
	private static int maxClique(Set<Long> clique, Set<Long> candidates, int minClique) {
		
		// pruning: filter candidates _ all candidates must have at least local degree minClique into the clique 
		// vertices, otherwise they could never participate in a larger clique as minClique
		Iterator<Long> iter = candidates.iterator();
		while (iter.hasNext()) {
			Long candidate = iter.next();
			if (getLocalDegree(candidate, clique)<clique.size()) {
				iter.remove();
			}
		}
		
		// pruning2: if clique size + candidates size smaller minClique, we can never achieve minClique
		if (clique.size()+candidates.size()<minClique) {
			return clique.size();
		}
//		System.out.println("Clique: " + clique);
//		System.out.println("Candidates: " + candidates);
//		System.out.println("minClique: " + minClique);
//		System.out.println();
		
		int max = clique.size();
		for (Long candidate : candidates) {
			if (isValidCliqueMember(candidate, clique)) {
				
				Set<Long> newClique = Util.copy(clique);
				newClique.add(candidate);
				
				Set<Long> newCandidates = Util.copy(candidates);
				newCandidates.remove(candidate);
				
				int temp = maxClique(newClique, newCandidates, Math.max(minClique, max));
				if (temp>max) {
					max = temp;
				}
			}
		}
		return max;
	}

	/**
	 * Returns true, if candidate is a potential clique member (i.e.,
	 * it knows all clique vertices).
	 * @param candidate
	 * @param clique
	 * @return
	 */
	private static boolean isValidCliqueMember(Long candidate, Set<Long> clique) {
		for (Long cliqueVertex : clique) {
			if (!graph.existsEdge(candidate, cliqueVertex)) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Returns the degree of this vertex w.r.t. all local likes.
	 * @param user_id
	 * @return
	 */
	private static int getLocalDegree(long user_id, Set<Long> likes) {
		int count = 0;
		for (Long like : likes) {
			if (graph.existsEdge(user_id, like)) {
				count++;
			}
		}
		return count;
	}
}
