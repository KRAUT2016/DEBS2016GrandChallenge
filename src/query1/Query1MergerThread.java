package query1;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import management.ManagementMain;
import common.events.Post;
import common.events.PostInactive;
import query1.datatypes.PostScore;
import query1.datatypes.UpdateNotification;
import util.Constants;
import util.Statistics;
import util.Time;

/**
 * merges the updates from the Worker Threads into their timely order
 * 
 * the incoming streams from the workers contain timestamp, post_id and newScore
 * 
 * streams the outstream
 * @author mayerrn
 *
 */
public class Query1MergerThread implements Runnable{

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
	
//	@SuppressWarnings("unchecked")
//	private LinkedBlockingQueue<UpdateNotification>[] updateStreams = 
//			new LinkedBlockingQueue[Constants.Q1_NUM_WORKERS];	
//
//	private UpdateNotification[] firstStreamEntries = new UpdateNotification[Constants.Q1_NUM_WORKERS];
//	
//	/*
//	 * post scores are new objects that reflect updates computed by the merger
//	 * a post score is always a "snapshot" of information when the worker processed an event
//	 */
//	private HashMap<Long,PostScore> postScores = new HashMap<Long, PostScore>(100000);  // scores of all active posts
//	
//	private long[][] top3 = new long[3][2]; // [score][post_id] the top 3 posts from all posts
//	private long[][] top3UpdatedPosts = new long[3][2]; // the best 3 from the currently updated posts
//	
//	/*
//	 * out stream 
//	 */
//	private BufferedWriter out;
//	private Time timeParser = new Time();
//	
//	/*
//	 * post infos are - most of all - immutable information about a post, e.g., information about the author
//	 * is just a reference to the splitter
//	 */
//	private ConcurrentHashMap<Long,Post> postinfos;
//	
//	/*
//	 * Latency metrics
//	 */
//	long latencyPerEventSummed = 0;
//	long numberOfEvents = 0;
//	
//	public Query1MergerThread() {
//		
//		try {
//			this.out = new BufferedWriter(new OutputStreamWriter(
//			          new FileOutputStream(Constants.Q1_OUTFILE), "utf-8"));
//		} catch (UnsupportedEncodingException | FileNotFoundException e1) {
//			e1.printStackTrace();
//		}
//		
//		for (int i = 0; i < Constants.Q1_NUM_WORKERS; i++){
//			LinkedBlockingQueue<UpdateNotification> stream = new LinkedBlockingQueue<UpdateNotification>(10);
//			updateStreams[i] = stream;
//		}
//		
//		this.postinfos = Query1Manager.getSplitter().getPostInfos();
//	}
//	
//	/**
//	 * fill the firstStreamEntries arrays with the first events from the updateStreams
//	 */
//	private void initializeFirstStreamEntries(){
//		try {
//			for (int i = 0; i < Constants.Q1_NUM_WORKERS; i++){
//				firstStreamEntries[i] = updateStreams[i].take();
//			}	
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	/**
//	 * compare two posts: can the first post take the position of the second post in a ranking?
//	 * @param first first post 
//	 * @param second second post 
//	 * @return true if first post is "higher", false if second post is "higher"; true also if the posts have the same id
//	 */
//	private boolean comparePosts(Post first, Post second){
//		
//		if (first.getPost_id() == second.getPost_id()){
//			return true;
//		}
//		long timeStampFirst = first.getTs();
//		long timeStampSecond = second.getTs();
//		if (timeStampFirst > timeStampSecond){
//			return true;
//		}
//		else if (timeStampFirst <timeStampSecond){
//			return false;
//		}
//		else { // timeStampFirst == timeStampSecond
//			// a draw: now compare the timestamps of the comments: latest comment first
//			LinkedList<Long> commentTimestampsFirst = first.getCommentTimestamps();
//			LinkedList<Long> commentTimestampsSecond = second.getCommentTimestamps();
//			Iterator<Long> iteratorFirst = commentTimestampsFirst.descendingIterator();
//			Iterator<Long> iteratorSecond = commentTimestampsSecond.descendingIterator();
//			while(iteratorFirst.hasNext()){
//				long firstPostCommentTs = iteratorFirst.next();
//				if (iteratorSecond.hasNext()){
//					long secondPostCommentTs = iteratorSecond.next();
//					if (firstPostCommentTs > secondPostCommentTs){
//						return true;
//					}
//					else if (firstPostCommentTs < secondPostCommentTs){
//						return false;
//					}
//					// else: there is still a tie; check the next event; proceed in the tiebreaker loop
//				}
//				else { // the second post has no more comments; the first post wins
//					return true;
//				}
//			}
//			// the first post does not have more comments; thus, it loses the comparison
//			return false;
//		}
//	}
//	
//	/**
//	 * update a given top 3 list with a new entry, if that entry is to be in the top 3 list
//	 * @param toBeChangedTop3 the top3 list to be updated
//	 * @param newScore the score of new entry
//	 * @param newpostId the id of new entry
//	 * @return true if the order has changed in the referenced array
//	 */
//	private boolean updateTop3Array(long[][] toBeChangedTop3, long newScore, long newpostId){
//		boolean result = false;
//		
//		int oldPlaceOfNewPost = -1;
//		/*
//		 * first: check if the updated post already is top2
//		 * 
//		 * if yes, there is a different procedure
//		 */
//		for (int r = 0; r <= 1; r++){
//			if (newpostId == toBeChangedTop3[r][1]){
//				oldPlaceOfNewPost = r;
//				break;
//			}
//		}
//		
//		if (oldPlaceOfNewPost != -1){			
//			/*
//			 * find the new place of the new post
//			 */
//			for (int r = 0; r <= 2; r++){
//				if (newScore < toBeChangedTop3[r][0]){
//					continue; // the new post is not in r
//				}
//				if  (newScore == toBeChangedTop3[r][0]){
//					// a draw: figure out if the new post is better than the current ranked r
//					Post newEntry = this.postinfos.get(newpostId);
//					Post oldEntry =  this.postinfos.get(toBeChangedTop3[r][1]);
//					
//					if (this.comparePosts(newEntry, oldEntry) == false){ 
//						continue; // the new post is not in r
//					}
//				}
//				// if continue was not triggered, then the post indeed is in r
//				/*
//				 * check difference between oldplace and r (new place)
//				 */
//				if (oldPlaceOfNewPost == r){
//					// just change the score
//					toBeChangedTop3[r][0] = newScore;
//					return false; // no change in ranking
//				}
//				else if (oldPlaceOfNewPost > r){ // post has improved ranking: old ranking was worse, r is better --
//					/*
//					 * deteriorate everything between old and new ranking
//					 */
//					for (int k = oldPlaceOfNewPost; k >= r; k--){
//						/*
//						 * start from the bad rankings:
//						 * copy the better ranking to the worse ranking
//						 */
//						toBeChangedTop3[k][0] = toBeChangedTop3[k-1][0];
//						toBeChangedTop3[k][1] = toBeChangedTop3[k-1][1];
//					}
//					/*
//					 * insert the new post in its place
//					 */
//					toBeChangedTop3[r][0] = newScore;
//					toBeChangedTop3[r][1] = newpostId;
//					return true;
//				}
//				else { // post has deteriorated ranking: old ranking was better, r is worse ++
//					/*
//					 * improve everything between old and new ranking
//					 */
//					for (int k = oldPlaceOfNewPost; k < r; k++){
//						/*
//						 * start from the good rankings:
//						 * copy the worse ranking to the good ranking 
//						 */
//						toBeChangedTop3[k][0] = toBeChangedTop3[k+1][0];
//						toBeChangedTop3[k][1] = toBeChangedTop3[k+1][1];
//					}
//					/*
//					 * insert the new post in its place
//					 */
//					toBeChangedTop3[r][0] = newScore;
//					toBeChangedTop3[r][1] = newpostId;
//					return true;
//				}
//			}
//		}
//		
//		else{ // the post was not in top3 before: normal procedure
//			
//			for (int r = 0; r <= 2; r++){ // r is the currently checked rank							
//				if (newScore < toBeChangedTop3[r][0]){
//					continue; // the new post is not in r
//				}
//				if  (newScore == toBeChangedTop3[r][0]){
//					// a draw: figure out if the new post is better than the current ranked r
//					Post newEntry = this.postinfos.get(newpostId);
//					Post oldEntry =  this.postinfos.get(toBeChangedTop3[r][1]);
//					
//					if (this.comparePosts(newEntry, oldEntry) == false){
//						continue; // the new post is not in r
//					}
//				}
//				// if continue was not triggered, then the post indeed is in r
//				
//				/*
//				 * change the rankings
//				 */
//				result = true;
//				
//				/*
//				 *  reorder the posts in the top 3: shift everything below the rankofUpdatedPost one rank lower
//				 *  for rank 0 and rank 1; rank 2 is not shifted lower any further (because rank3 is not of any interest)
//				 *  
//				 *  exception: stop at the old rank of the new post; shifting down stops there
//				 */
//				for (int k = 1; k >= r; k--){
//					/*
//					 * rank 2 <-- rank 1 if k = 1; rank 1 <-- rank 0 if k = 0
//					 */
//					toBeChangedTop3[k+1][0]= toBeChangedTop3[k][0];
//					toBeChangedTop3[k+1][1] = toBeChangedTop3[k][1];
//				}				
//							
//				/*
//				 * put the post into its rank
//				 */
//				toBeChangedTop3[r][0] = newScore;
//				toBeChangedTop3[r][1] = newpostId;
//				return result;	
//			}
//		}
//		return result;
//	}
//	
//	@Override
//	public void run() {
//		long timestamp = 0;
//		long creationtime = 0;
//		/*
//		 * get latest entry from each stream
//		 * 
//		 * update the current score of active posts
//		 * 
//		 * recompute the top-3 - if they have changed, produce an output
//		 */
//		while (true){
//			boolean top3changed = false;
//			top3UpdatedPosts = new long[3][2];
//			
//			this.initializeFirstStreamEntries(); // fill first stream entries datastructure
//			timestamp = firstStreamEntries[0].getTimestamp(); // timestamp of the currently processed updates
//			creationtime = firstStreamEntries[0].getCreationTime(); // creation time of the event that caused the currently processed updates
//			
//			if (timestamp == -1){
//				break; // Merger shuts down
//			}
////			System.out.println(timestamp);
//			boolean caseA = true; // are there no negative updates to top3 entry scores
//			
//			/*
//			 * depending on the case, handle the updates to recomputed top3:
//			 * case A: all top3 entries got there because they gained more points or stayed at the same score
//			 * case B: one or more top3 entries lost scores or became inactive -- for this reason, the replacement in top3 could be any
//			 * post, also a post that wasn't updated
//			 * 
//			 * in case A, only the top3 updated posts are considered for the global top3
//			 * in case B, any post can be in top3 
//			 * -- this results in a run through the complete set of active posts
//			 */
//						
//			/*
//			 * run through the updates: get the top3 active posts of the updated posts
//			 * and update all postScores 
//			 */
//			for (int i = 0; i < firstStreamEntries.length; i++){
//				HashMap<Long, PostScore> updates = firstStreamEntries[i].getUpdates();
//				
//				for (Map.Entry<Long, PostScore> index : updates.entrySet()){
//					
//
//					int newScoreOfUpdatedPost = index.getValue().getTotalScore();
//										
//										
//					/*
//					 * check if current update is positive or negative
//					 */
//					int delta = 0;
//					PostScore oldScoreOfUpdatedPost = this.postScores.get(index.getKey());
//					if (oldScoreOfUpdatedPost != null){
//						delta = newScoreOfUpdatedPost - oldScoreOfUpdatedPost.getTotalScore();
//					}
//					else{
//						delta = newScoreOfUpdatedPost;
//					}
//					
//					
//					
//					/*
//					 * if there  wasn't a top3 score decremented by the update yet (caseA),
//					 * check if it is the case now.
//					 */
//					if (caseA && (delta < 0)){
//						// check if the post losing scores was in the top3
//						for (int j = 0; j <=2; j++){
//							if (top3[j][1] == index.getKey()){ // indeed, the post with negative delta was in the top3!
//								caseA = false;
//								break;
//							}
//						}
//					}
//					
//					
//					/*
//					 * update postScores data structure: active and inactive posts
//					 */
//					if (newScoreOfUpdatedPost != 0){
//						// put the updated post into the postScores
//						this.postScores.put(index.getKey(), index.getValue());
//						/*
//						 * put current update in the top3UpdatedPosts
//						 */
//						this.updateTop3Array(top3UpdatedPosts, newScoreOfUpdatedPost, index.getKey()); // the return value doesn't matter for the updatedposts array
//					}
//					else {
//						// if post is inactive, delete it from the postScores
//						this.postScores.remove(index.getKey());
//						
//						// notify the splitter to safely remove the inactive post from internal data structure
//						PostInactive postInactive = new PostInactive(index.getKey());
//						try {
//							Query1Manager.getSplitter().getInactivePostNotifications().put(postInactive);
//						} catch (InterruptedException e) {
//							e.printStackTrace();
//						}
//					}
//					
//				}
//			}				
//			
//			/*
//			 * build the new top3 list from old top3 list
//			 */
//			long[][] newTop3 = new long[3][2];
//			for (int l = 0; l <= 2; l++){
//				if (top3[l][1] != 0){ // the place is filled, so it is not 0
//					this.updateTop3Array(newTop3, this.postScores.get(top3[l][1]).getTotalScore(), top3[l][1]);				
//				}
//			}
//
//			if (caseA){ // case A: all top3 entries gained more points or stayed at the same score 
//				/*
//				 * check whether a top3UpdatedPost fits into the new top3 now
//				 */
//				for (int m = 0; m <= 2; m++){ 				
//					if (top3UpdatedPosts[m][1] != 0){ // exclude post_id 0, which is an empty field
//						this.updateTop3Array(newTop3, top3UpdatedPosts[m][0], top3UpdatedPosts[m][1]);
//					}
//					
//				}
//			}
//			else { // case B: one or more top3 entries lost scores or became inactive
//				for (Map.Entry<Long, PostScore> index : postScores.entrySet()){
//					// check for each active post if it fits to the new top3 now
//					this.updateTop3Array(newTop3, index.getValue().getTotalScore(), index.getKey());					
//				}
//			}
//
//			
//			/*
//			 * check if top3 have changed	
//			 */
//			for (int i = 0; i <=2; i++){
//				if (top3[i][1] != newTop3[i][1]){
//					top3changed = true;
//					break;
//				}
//			}
//								
//			/*
//			 * if top3 have changed, create an output
//			 */
//			if (top3changed){
//				// output the top 3, timestamp of the current step.
//				try {
//					StringBuilder outstring = new StringBuilder();
//					outstring.append(timeParser.timeStamp(timestamp));
//					for (int i = 0; i <=2; i++){
//						if (newTop3[i][1] == 0){
//							outstring.append(",-,-,-,-"); // no post_id, no post_user, no post_score, no post_commenters
//							continue; // there is no entry for this position
//						}
//						outstring.append(',');
//						outstring.append(newTop3[i][1]);
//						outstring.append(',');
//						outstring.append(this.postinfos.get(newTop3[i][1]).getUser());
//						outstring.append(',');
//						outstring.append(this.postScores.get(newTop3[i][1]).getTotalScore());
//						outstring.append(',');
//						outstring.append(this.postScores.get(newTop3[i][1]).getNumberOfCommenters() - 1);
//					}
//					outstring.append(System.lineSeparator());
//					
//					/*
//					 *  latency measurement
//					 */
//					numberOfEvents++;
//					long currentEventLatency = System.nanoTime() - creationtime; 
//					latencyPerEventSummed += currentEventLatency;
//					this.out.write(outstring.toString());
//					
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//				
//			}
//			
//			top3 = newTop3;
//		}
//		
//		try {
//			/*
//			 * Close writers and write latency statistics to Statistics module.
//			 */
//			this.out.close();
//			long totalTime = (System.currentTimeMillis()-ManagementMain.getStartProcessingTime());
//			double averageLatencyQ1 = (double)latencyPerEventSummed/(double)numberOfEvents;
//			Statistics.setDurationQ1(totalTime);
//			Statistics.setAverageLatencyQ1(averageLatencyQ1);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
////		System.out.println("Query1Merger closed.");
//	}
//
//	public LinkedBlockingQueue<UpdateNotification>[] getUpdateStreams() {
//		return updateStreams;
//	}
//
//	public void setUpdateStreams(LinkedBlockingQueue<UpdateNotification>[] updateStreams) {
//		this.updateStreams = updateStreams;
//	}

}
