package query2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The global data structure for the friendship relation,
 * so that graph updates have to be performed only once.
 * TODO: optimize to use only primitives "int" instead of Objects "Long"
 * @author mayercn
 *
 */
public class Graph {

	private HashMap<Long, Set<Long>> adjacencyList = new HashMap<Long, Set<Long>>(10000);
	private int edges = 0;
	
	public Graph(String filename) throws NumberFormatException, IOException {
		BufferedReader in = new BufferedReader(new FileReader(new File(filename)));
		String line = null;
		while ((line=in.readLine())!=null) {
			String[] s = line.split("\\s+");
			long user1 = Long.valueOf(s[0]);
			long user2 = Long.valueOf(s[1]);
			addVertex(user1);
			addVertex(user2);
			addEdge(user1, user2);
		}
		in.close();
	}
	
	public Graph() {
		
	}

	/**
	 * Adds a vertex to the graph structure
	 * @param user
	 */
	public void addVertex(long user) {
		if (!adjacencyList.containsKey(user)) {
			adjacencyList.put(user, new HashSet<Long>());
		}
//		System.out.println("Graph size: " + adjacencyList.size() + " vertices; " + edges + " edges");
	}
	
	/**
	 * Adds an undirected edge (user1,user2) to the graph.
	 * @param user1
	 * @param user2
	 */
	public void addEdge(long user1, long user2) {
		if (!adjacencyList.containsKey(user1) || !adjacencyList.containsKey(user2)) {
			System.err.println("Error: Cannot add edge, because users do not exist");
		}
		if (!adjacencyList.get(user1).contains(user2)) {
			adjacencyList.get(user1).add(user2);
			edges++;
		}
		if (!adjacencyList.get(user2).contains(user1)) {
			adjacencyList.get(user2).add(user1);
			edges++;
		}
	}
	
	/**
	 * Returns true, if there is an edge between u and v in the graph.
	 * @param u
	 * @param v
	 * @return
	 */
	public boolean existsEdge(long u, long v) {
		Set<Long> neighbors = adjacencyList.get(u);
		if (neighbors==null) {
			return false;
		} else {
			return neighbors.contains(v);
		}
	}
	
	/**
	 * Returns the degree of vertex u.
	 * @param u
	 * @return
	 */
	public int getDegree(long u) {
		return adjacencyList.get(u).size();
	}

	/**
	 * Returns all neighbors of vertex u
	 * @param user_id
	 * @return
	 */
	public Set<Long> getNeighbors(long u) {
		return adjacencyList.get(u);
	}

	public Set<Long> getVertices() {
		return adjacencyList.keySet();
	}
	
}
