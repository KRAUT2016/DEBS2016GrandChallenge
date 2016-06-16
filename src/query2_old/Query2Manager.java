package query2_old;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import util.Constants;
import util.Time;
import management.ManagementMain;
import management.Serializer;
import common.events.AbstractEvent;
import common.events.Comment;
import common.events.CommentExpires;
import common.events.EventType;
import common.events.Friendship;
import common.events.Like;
import common.events.Marker;
import common.events.StreamClosed;

/**
 * Receives event streams friendship (last TS: 1350740269000), likes, comments and starts workers to find comments
 * with largest "like-cliques".
 * @author mayercn
 *
 */
public class Query2Manager extends Thread {

//	private List<CommentProcessor> commentProcessors;
	
	private ArrayList<Query2WorkerThread> workers = new ArrayList<Query2WorkerThread>();
	private Query2MergerThread merger;
	private Time time;
	private Graph graph;
	
	/*
	 * Needed to synchronize friendship updates on the shared graph structure.
	 * The manager waits for all workers to call markerCallback() until it is
	 * sure that all workers are stale. Only then, it updates the graph.
	 */
	private int markerCounter;

	public Query2Manager() {
		time = new Time();
		if (!Constants.Q2_REPLICATE_GRAPH) {
			graph = new Graph();
		}
	}
	
	@Override
	public void run() {
		//System.out.println("DEBUG: starting the query2 manager");
		ArrayBlockingQueue<AbstractEvent> inQ = Serializer.query2Queue;
		LinkedBlockingQueue<AbstractEvent> expiresQ = new LinkedBlockingQueue<AbstractEvent>();
		
		long lastTs = -1;
		
		/*
		 * start workers and merger
		 */
		for (int i = 0; i < Constants.Q2_NUM_WORKERS; i++){
			Query2WorkerThread newWorker = new Query2WorkerThread(this);
			workers.add(newWorker);
			newWorker.start();
		}
		merger = new Query2MergerThread(workers);
		merger.start();
		
		AbstractEvent nextEvent = null;
		AbstractEvent inQEvent = null;
		AbstractEvent expiresEvent = null;
		int total = 0;

		runloop:
		while (true) {
			try {
				
				/*
				 * Determine next event, i.e., the event with smallest TS of both queues
				 */
				if (inQEvent==null) {
					inQEvent = inQ.take();			// block and wait for next event, remove it
					
					if (inQEvent.getTs()>=lastTs) {
						lastTs = inQEvent.getTs();
					} else {
						System.err.println("Error: Serializer not in order!!!");
						System.err.println("lastts: " + lastTs);
						System.err.println("Event ts: " + inQEvent.getTs());
						System.err.println("Event type: " + inQEvent.getType());
//						System.err.println("TS: " + time.timeStamp(inQEvent.getTs()));
						System.exit(-1);
					}
					
					/*
					 * Directly add expires event before a comment is processed.
					 */
					if (inQEvent.getType()==EventType.COMMENT) {
						// add new expires comment event to in queue
						Comment comment = (Comment) inQEvent;
						long expiresTs = comment.getTs()+Constants.Q2_D*1000;
						CommentExpires expires = new CommentExpires(expiresTs, comment.getComment_id(),
								comment.getCreationTime());
						expiresQ.put(expires);
					}
				}
				if (expiresEvent==null) {
					expiresEvent = expiresQ.poll();	// poll for expiring event and remove it
				}
				if (expiresEvent!=null && expiresEvent.getTs()<inQEvent.getTs() 
						&& inQEvent.getType()!=EventType.STREAM_CLOSED) {
					nextEvent = expiresEvent;
					/*
					 *  set creation time of the comment expires event to the creation time of
					 *  the event that caused the comment to expire.
					 */
					nextEvent.setCreationTime(inQEvent.getCreationTime());
					expiresEvent = null;
				} else {
					nextEvent = inQEvent;
					inQEvent = null;
				}
				
				total++;
//				System.out.println("new event from inQ: " + nextEvent.getType() + " TS: " + nextEvent.getTs());
//				System.out.println("inQ size: " + inQ.size());
//				System.out.println("expiresQ size: " + expiresQ.size());
//				System.out.println("total events processed: " + total);
//				System.out.println("average processing time per event: " 
//						+ (double)(System.currentTimeMillis()-ManagementMain.getStartProcessingTime())/(double)total + " ms");
//				System.out.println();
//				if (total==1000000) {
//					System.exit(-1);
//				}
				
				switch (nextEvent.getType()) {
				case FRIENDSHIP:
					Friendship friendship = (Friendship) nextEvent;
					processFriendship(friendship);
					break;
				case COMMENT:
					Comment comment = (Comment) nextEvent;
					processComment(comment);
					break;
				case COMMENT_EXPIRES:
					CommentExpires commentExpires = (CommentExpires) nextEvent;
					processCommentExpires(commentExpires);
					break;
				case LIKE:
					Like like = (Like) nextEvent;
					processLike(like);
					break;
				case STREAM_CLOSED:
					try {
						for (Query2WorkerThread worker: this.workers){
							worker.getInQueue().put((StreamClosed) nextEvent);
						}
					}
					catch (InterruptedException e) {
						e.printStackTrace();
					}					
					break runloop; // leave the run loop, shut down Query2Manager
				default:
					break;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
//		System.out.println("Query2Manager thread shut down.");
	}

	/**
	 * Forward event to worker that is responsible for this comment.
	 * @param commentExpires
	 */
	private void processCommentExpires(CommentExpires commentExpires) {
		
		/*
		 * associate the comment to the worker thread that handles the post the comment is associated with
		 */
		int workerId = (int) (commentExpires.getComment_id() % Constants.Q2_NUM_WORKERS);
		try {
			this.workers.get(workerId).getInQueue().put(commentExpires);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Update all comments with the new friendship request.
	 * As the friendship requests changes the global data structure,
	 * we wait for all workers to finish work using a marker event.
	 * @param friendship
	 */
	private void processFriendship(Friendship friendship) {
		
		if (Constants.Q2_REPLICATE_GRAPH) {
				
			/*
			 * step 1: send friendship to all workers
			 */
			for (Query2WorkerThread worker: this.workers){
				try {
					worker.getInQueue().put(friendship);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} else {
			synchronized(this) {	// always call wait() only on locked objects
	
				/*
				 * step 1: send a marker to all workers
				 */
		
				Marker marker = new Marker();
				for (Query2WorkerThread worker : workers) {
					try {
						worker.getInQueue().put(marker);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				/*
				 * step 2: wait for all marker "returns"
				 */
				markerCounter = 0;
				while (markerCounter!=Constants.Q2_NUM_WORKERS) {
					try {
						this.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			
			/*
			 * step 3: update graph
			 */
			graph.addVertex(friendship.getUser_id_1());
			graph.addVertex(friendship.getUser_id_2());
			graph.addEdge(friendship.getUser_id_1(), friendship.getUser_id_2());
			
			/*
			 * step 4: send friendship to all workers
			 */
			for (Query2WorkerThread worker: this.workers){
				try {
					worker.getInQueue().put(friendship);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * To be called by workers to inform the manager
	 * that all the events up to the marker have been processed. 
	 * Then, the manager can safely update the shared data structure "Graph".
	 */
	public synchronized void markerCallback() {
		markerCounter++;
		this.notify();	// as the method is synchronized, the executing thread has the lock on "this"
	}
	
	
	/**
	 * Add new comment to the data structures. Associate the comment
	 * to the worker thread that handles it.
	 * @param comment
	 */
	private void processComment(Comment comment) {
		int workerId = (int) (comment.getComment_id() % Constants.Q2_NUM_WORKERS);
		try {
			this.workers.get(workerId).getInQueue().put(comment);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Update the worker thread that handles the comment to be liked.
	 * @param like
	 */
	private void processLike(Like like) {
		
		if (!Constants.Q2_REPLICATE_GRAPH) {
			graph.addVertex(like.getUser_id());
		}
		
		int workerId = (int) (like.getComment_id() % Constants.Q2_NUM_WORKERS);
		try {
			this.workers.get(workerId).getInQueue().put(like);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	

	public Graph getGraph() {
		return graph;
	}

	public void setGraph(Graph graph) {
		this.graph = graph;
	}
}
