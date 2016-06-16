package query1;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import common.events.AbstractEvent;
import common.events.Comment;
import common.events.Marker;
import common.events.Post;
import common.events.PostInactive;
import query1.datatypes.PostScore;
import query1.datatypes.TimeTableEntry;
import query1.datatypes.UpdateNotification;
import util.Constants;

/**
 * Multiple instances of this worker thread are initialized
 * 
 * a worker thread is responsible for a certain set of active posts and the corresponding comments
 * 
 * a worker thread computes the updates of scores of its active posts
 * 
 * the following things can happen: *
 * a) a new post or associated comment induces an update of +10 points
 * b) 24h since creation of a post or comment has passed , inducing an update of -1 point
 *  
 * output to the Merger: Posts that contain an updated timestamp and updated total score 
 * (internal timestamp of post is set to the event that triggers the update)
 * 
 * @author mayerrn
 *
 */
public class Query1WorkerThread implements Runnable{

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
//	private static int workerIdCounter = 0; // to assign incremental ids to the workers upon construction
//	private int workerId; // decides about which posts are assigned to the worker
//	
//	private LinkedBlockingQueue<Object> inQueue = new LinkedBlockingQueue<Object>(10); // incoming events for the worker
//		
//	private HashMap<Long,PostScore> postScores = new HashMap<Long,PostScore>(100000); // scores of posts <post_id, postscore>
//	
//	private HashMap<Long, HashSet<Long>> postCommentersSets = new HashMap<Long, HashSet<Long>>(100000); // all distinct commenter's user's ids of a post <post_id, commenters>
//	
//	private TimeTableEntry lastEntry;
//	
//	private long currentSimulationTime;
//		
//	
//	/**
//	 * handle a post
//	 * @param post the post to be handled
//	 */
//	private UpdateNotification handlePost(Post post){
//		return this.progress(post.getTs(), post, null, post.getCreationTime());		
//	}
//	
//	/**
//	 * handle a comment
//	 * @param comment the comment to be handled
//	 */
//	private UpdateNotification handleComment(Comment comment){		
//		return this.progress(comment.getTs(), null, comment, comment.getCreationTime());
//	}
//	
//	/**
//	 * 
//	 * progress in time: from currentSimulationTime to newSimulationTime
//	 *
//	 * 
//	 * @param newSimulationTime
//	 * @param currentlyProcessedPost post that triggered this simulation step, if any
//	 * @param currentlyProcessedComment comment that triggered this simulation step, if any
//	 * @return all new postscores of updated Posts
//	 */
//	private HashMap<Long,PostScore> updateSimulationTime(long newSimulationTime, Post currentlyProcessedPost, Comment currentlyProcessedComment){
//		HashMap<Long,PostScore> result = new HashMap<Long,PostScore>();
//		HashSet<Long> updatedPostIds = new HashSet<>();
//				
//		long progresstime = newSimulationTime - this.currentSimulationTime; // the time has progressed so much in this simulation step
////		System.out.println("progresstime " + progresstime / 3600000.0);
//		
//		if (this.lastEntry != null){
//			while (true){
//				/*
//				 * from the last simulation step to the next entry, how much time would pass
//				 */
//				long distanceToNextEntry = this.lastEntry.getSuccessor().getDayTime() - (this.currentSimulationTime % Constants.MS_24H);
////				System.out.println("distance to next entry: " + distanceToNextEntry / 3600000.0);
//				if (distanceToNextEntry <= 0){ // the daytime of next entry is before the daytime of current simulation time; 24 h - distance pass;
//					distanceToNextEntry = Constants.MS_24H + distanceToNextEntry;
//				}
////				System.out.println("distance to next entry: " + distanceToNextEntry / 3600000.0);
//				progresstime -= distanceToNextEntry;
//				if (progresstime < 0){ // the step is not reached
//					break; // simulation done
//				}
//				// else; the next entry is reached
//				this.currentSimulationTime += distanceToNextEntry; // the simulation has progressed to the next entry
//
////				System.out.println("step reached " + (this.currentSimulationTime % Constants.MS_24H) / 3600000.0);
////				updatedPostIds.add(this.lastEntry.getSuccessor().getPost_id()); // the post id belongs to the updated posts // TODO when parallel version is needed, create a different time table entry class with post_id links instead of post links; or update these calls to access post id via the post assicated to time table entry
////				this.postScores.get(this.lastEntry.getSuccessor().getPost_id()).incrementTotalScore(-1); // remove one point from post's total score
//				boolean entryIsDead = this.lastEntry.getSuccessor().decrementTTL(); // decrement the TTL of next entry, check if next entry is dead
//				if (entryIsDead){ 
////					System.out.println("entry is dead");
//					this.lastEntry.setSuccessor(this.lastEntry.getSuccessor().getSuccessor());
//				}
//				this.lastEntry = this.lastEntry.getSuccessor(); // progress: the last entry visited is the next entry; 
//				// check in the while loop whether the next entry is between the two time stamps
//			}
//		}
//		
//		// simulation of the time ring done
//		this.currentSimulationTime = newSimulationTime; // simulation time of finishing the simulation
//		
//		/*
//		 * important: consider the processed post or event now
//		 */
//		if (currentlyProcessedPost != null){
////			System.out.println("new post " + (currentlyProcessedPost.getTs() % Constants.MS_24H) / 3600000.0);
//			
//			PostScore ps = new PostScore(10, 1);
//			this.postScores.put(currentlyProcessedPost.getPost_id(), ps);
//			HashSet<Long> pcset = new HashSet<Long>(); // post commenters set
//			pcset.add(currentlyProcessedPost.getUser_id()); // put the post author inside
//			this.postCommentersSets.put(currentlyProcessedPost.getPost_id(), pcset); // create a post commenters set
//			
//			/*
//			 * update the ring structure: insert a new entry
//			 */
////			TimeTableEntry newEntry = new TimeTableEntry(newSimulationTime, currentlyProcessedPost.getPost_id()); // TODO when parallel version is needed, create a different time table entry class with post_id links instead of post links; or update these calls to access post id via the post assicated to time table entry
////			if (this.lastEntry != null){
////				this.lastEntry.addNewSuccessor(newEntry);
////				this.lastEntry = newEntry;
////			}
////			else{
////				this.lastEntry = newEntry;
////				this.lastEntry.setSuccessor(this.lastEntry);
////			}
//			/*
//			 * any case: changed posts: add the new post
//			 */
//			updatedPostIds.add(currentlyProcessedPost.getPost_id());
//			/*
//			 * commenters set: create a new one with the post_id as commenter
//			 */
//			HashSet<Long> cs = new HashSet<Long>();
//			cs.add(currentlyProcessedPost.getPost_id());
//			
//		}
//		else if (currentlyProcessedComment != null){
//			
//			/*
//			 * update the ring structure: insert a new entry
//			 */
////			TimeTableEntry newEntry = new TimeTableEntry(newSimulationTime, currentlyProcessedComment.getPost_commented()); // TODO when parallel version is needed, create a different time table entry class with post_id links instead of post links; or update these calls to access post id via the post assicated to time table entry
////			this.lastEntry.addNewSuccessor(newEntry);
////			this.lastEntry = newEntry;
//			
//			/*
//			 * increment the postscore's totalscore by 10
//			 */
//			PostScore ps = this.postScores.get(currentlyProcessedComment.getPost_commented());
//			ps.incrementTotalScore(10);
//			/*
//			 * add the comment to the commenter's list
//			 */
//			HashSet<Long> commentersset =  this.postCommentersSets.get(currentlyProcessedComment.getPost_commented());
//			commentersset.add(currentlyProcessedComment.getUser_id());
//			/*
//			 * update the postscore's number of commenters
//			 */
//			ps.setNumberOfCommenters(commentersset.size());		
//			/*
//			 * any case: changed posts: add the post
//			 */
//			updatedPostIds.add(currentlyProcessedComment.getPost_commented());
//		}
//		
//		
//		/*
//		 * compute the updates: for each post whose score has changed, an update is computed
//		 */
//		for (Long updatedPost : updatedPostIds){
//			PostScore currentPostScore = this.postScores.get(updatedPost);
//			PostScore clonedPostScore = new PostScore(currentPostScore.getTotalScore(), currentPostScore.getNumberOfCommenters()); // clonse the object, as it can change in the worker at any time
//			result.put(updatedPost, clonedPostScore);
//		}
//		return result;
//	}
//
//	/**
//	 * progress the processing to the new timestamp:
//	 * - compute all updates happening until the new timestamp
//	 * - all those updates get the new timestamp assigned
//	 * - the updates are transmitted to the merger
//	 * @param newSimulationTime the new simulation time
//	 * @param newPost the new post, if any in this update step
//	 * @param newComment the new comment, if any in this update step
//	 * @return UpdateNotification to the Merger
//	 */
//	private UpdateNotification progress(long newSimulationTime, Post newPost, Comment newComment, long creationTime){
//		
//		HashMap<Long,PostScore> updates = this.updateSimulationTime(newSimulationTime, newPost, newComment); // returns <post_id, postScore information>
//		UpdateNotification result = new UpdateNotification(newSimulationTime, updates, creationTime);
//		return result; 
//	}
//			
//	
//	public Query1WorkerThread(){
//		this.workerId = workerIdCounter++;
//	}
//
//	public int getWorkerId() {
//		return workerId;
//	}
//
//	@Override
//	public void run() {
//				
//		UpdateNotification result = null;
//		final LinkedBlockingQueue<UpdateNotification> mergerInStream = Query1Manager.getMerger().getUpdateStreams()[workerId];
//			
//		runloop:
//		while(true){
//			try {
//
//				AbstractEvent event = (AbstractEvent) inQueue.take();
//				switch (event.getType()){
//					case COMMENT: result = this.handleComment((Comment) event); 
//						break;
//					case POST: result = this.handlePost((Post) event);
//						break;
//					case MARKER: result = this.progress(event.getTs(), null, null, event.getCreationTime());
//						break;
//					case STREAM_CLOSED: 
//						result = new UpdateNotification(-1, null, 0); 
//						mergerInStream.put(result);
//						break runloop; // worker shuts down 
//				}					
////			}
//				
//			/*
//			 * emit the result to the Merger
//			 */
//			mergerInStream.put(result);
//		
//				
//			} catch (InterruptedException e) { // if taking from inqueue was interrupted
//				e.printStackTrace();
//			}
//		}
////		System.out.println("Query1WorkerThread " + this.workerId + " shuts down.");
//	}
//
//	public LinkedBlockingQueue<Object> getInQueue() {
//		return inQueue;
//	}
//
//	public void setInQueue(LinkedBlockingQueue<Object> inQueue) {
//		this.inQueue = inQueue;
//	}
	
}
