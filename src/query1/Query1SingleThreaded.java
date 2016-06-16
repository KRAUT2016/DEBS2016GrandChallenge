package query1;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeSet;

import common.events.AbstractEvent;
import common.events.Comment;
import common.events.Post;
import management.ManagementMain;
//import management.Serializer;
import management.SerializerQ1;
//import management.SerializerQ2;
import query1.datatypes.TimeTableEntry;
import util.Constants;
import util.Logger;
import util.Statistics;
import util.Time;

public class Query1SingleThreaded implements Runnable {
	
	
	private int sortingScoreTH = -1;
	
	/**
	 * posts with score > TH are kept sorted for easier built of top3
	 */
	private TreeSet<Post> sortedPosts = new TreeSet<Post>();
	
//	private ArrayList<TreeSet<Post>> binsSortedPosts = new ArrayList<TreeSet<Post>>();
	
	/**
	 * posts with score <= TH are not sorted
	 */
	private HashSet<Post> unsortedPosts = new HashSet<Post>();
	
	
	/**
	 * post_id --> post
	 * 
	 */
	private LinkedHashMap<Long, Post> postId2Post = new LinkedHashMap<Long, Post>(100000, 0.5f);
	
	/**
	 * comment_id --> comment
	 */
	private LinkedHashMap<Long, Comment> commentId2Comment = new LinkedHashMap<Long, Comment>(100000, 0.5f);
	
	private SerializerQ1 serializer;
	

	// worker data structures
	private TimeTableEntry lastEntry;
	private long currentSimulationTime;
	private long lastSimulationTime;
	
	// merger data structures
	private long[][] top3 = new long[3][2]; // [score][post_id] the top 3 posts from all posts
//	private long[][] top3UpdatedPosts = new long[3][2]; // the best 3 from the currently updated posts
	
	/*
	 * out stream 
	 */
	private BufferedWriter out;
	private Logger eventLatencyLogger = new Logger("eventLatenciesQ1.dat");
	private Time timeParser = new Time();
	
	/*
	 * Latency metrics
	 */
	long latencyPerEventSummed = 0;
	long numberOfEvents = 0;
	
	/**
			 * the first entry visited in this update is kept in order to determine whether a step of 0 time means
			 * that 24 hours have passed, or 0 hours have passed;
			 * 
			 * if the first entry visited is seen a second time, 24 hours have passed.
			 * else, 0 hours have passed.
	 */
	TimeTableEntry firstEntryVisited = null;
	
	public Query1SingleThreaded(SerializerQ1 serializer){
		this.serializer = serializer;
		try {
			this.out = new BufferedWriter(new OutputStreamWriter(
			          new FileOutputStream(Constants.Q1_OUTFILE), "utf-8"));
		} catch (UnsupportedEncodingException | FileNotFoundException e1) {
			e1.printStackTrace();
		}
	}
	
	/**
	 * 
	 * progress in time: from currentSimulationTime to newSimulationTime
	 *
	 * 
	 * @param newSimulationTime
	 * @param currentlyProcessedPost post that triggered this simulation step, if any
	 * @param currentlyProcessedComment comment that triggered this simulation step, if any
	 * @return all updated posts
	 */
	private LinkedList<Post> updateSimulationTime(long newSimulationTime, Post currentlyProcessedPost, Comment currentlyProcessedComment){
		LinkedList<Post> result = new LinkedList<Post>();
		
		
		long progresstime = newSimulationTime - this.currentSimulationTime; // the time has progressed so much in this simulation step
			
		boolean postsHaveDied = false; // posts have died and this has not been reflected in an update yet.
		boolean nextEntryIsDead = false; // entry has died and not been reflected in update of timetable yet
		TimeTableEntry diedEntry = null;
		
		if (this.lastEntry != null){
			
			
			firstEntryVisited = this.lastEntry.getSuccessor();
			
			while (true){
				
				
				final Post postOfNextEntry = this.lastEntry.getSuccessor().getPost();
				
				final TimeTableEntry nextEntry = this.lastEntry.getSuccessor();
				
				/*
				 * from the last simulation step to the next entry, how much time would pass on the wall-clock
				 */
				long daytimeDistanceToNextEntry = nextEntry.getDayTime() - ( this.currentSimulationTime % Constants.MS_24H);
				
				/*
				 * how many hours on the clock pass between the entries
				 */
				if (daytimeDistanceToNextEntry < 0){ // the daytime of next entry is before the daytime of current simulation time; 24 h - distance pass;
					daytimeDistanceToNextEntry = Constants.MS_24H + daytimeDistanceToNextEntry;
				}
				else if (daytimeDistanceToNextEntry == 0){
					/*
					 * exactly 24 hours have passed when the first entry visited in this update is visited again
					 */
					if (nextEntry == firstEntryVisited){ 
						daytimeDistanceToNextEntry = Constants.MS_24H;
					}
					// else, 0 hours have passed; another entry with the same daytime timestamp is reached 
					else {
						daytimeDistanceToNextEntry = 0;
					}
				}
				

				progresstime -= daytimeDistanceToNextEntry;
				
				if (progresstime < 0){ // the step is not reached
					break; // simulation done
				}
				else{ // the step is reached; hence, the simulation time is updated
					this.currentSimulationTime += daytimeDistanceToNextEntry;
				}
				
				/*
				 * if there was progress and posts have died that haven't been reflected in an update yet,
				 * send out an intermediate update now
				 * 
				 * it is before the current simulation time
				 */
				if (daytimeDistanceToNextEntry != 0 && postsHaveDied){
					
					
					if (currentlyProcessedComment != null){
						this.computeNewTop3(result, this.lastSimulationTime, currentlyProcessedComment.getCreationTime());
					}
					else if (currentlyProcessedPost != null){
						this.computeNewTop3(result, this.lastSimulationTime, currentlyProcessedPost.getCreationTime());
					}
					else { // update was triggered by end of streams : end of stream does not have a creation time, hence current time is taken
						this.computeNewTop3(result, this.lastSimulationTime, System.nanoTime());
					}
					
					result = new LinkedList<>(); // result has been flushed; hence, the result is gathered anew for the rest of the simulation step
					postsHaveDied = false;
				}
				
				
				if (nextEntryIsDead){
					nextEntryIsDead = false;
					firstEntryVisited = nextEntry; // now, set the first entry visited to the dead entry's successor
				
					// check whether the time table is empty
					
					if (diedEntry == this.lastEntry){ // if the died entry is reached again in the succeeding step, there are no other entries than the died one
						this.lastEntry = null; // time table empty
						break; // there is no entry left in the time table.
					}
				}
				boolean entryIsDead = nextEntry.decrementTTL(); // decrement the TTL of next entry, check if next entry is dead
					
				boolean postIsDead = postOfNextEntry.incrementPostScore(-1); // remove one point from post's total score
				
				
				if (entryIsDead){
					nextEntryIsDead = true;
					diedEntry = nextEntry;
					long commenter_user_id = diedEntry.getCommenter_user_id();
					if (commenter_user_id != Long.MIN_VALUE && commenter_user_id != diedEntry.getPost().getUser_id()){
						diedEntry.getPost().getCommenters().remove(commenter_user_id);
					}
					this.lastEntry.setSuccessor(nextEntry.getSuccessor());
				}
				else{				
				
					this.lastEntry = nextEntry; // progress: the last entry visited is the next entry;
				}
				
				if (postOfNextEntry.isUpdateThisRound() == false){
					result.add(postOfNextEntry); // the post belongs to the updated posts
					postOfNextEntry.setUpdateThisRound(true);
				}
				
				// if post is dead remove it from data structures
				if ( postIsDead ){
					
					postsHaveDied = true;

					
					// delete post from postId2Post map
					this.postId2Post.remove(postOfNextEntry.getPost_id());
					// delete associated comments from commentId2Comment map
					for (long comment_id : postOfNextEntry.getComments()){
						this.commentId2Comment.remove(comment_id);
					}
						
					
				}
				


				// check in the while loop whether the next entry is between the two time stamps
				// the next entry is reached
				
				this.lastSimulationTime = this.currentSimulationTime;
				
			}
		}
		
		// simulation of the time step done
		this.currentSimulationTime = newSimulationTime; // timestamp of finishing the simulation step
		this.lastSimulationTime = this.currentSimulationTime; // save the last simulation time

		
		/*
		 * important: consider the processed post or event now
		 */
		if (currentlyProcessedPost != null){
			
			currentlyProcessedPost.setPostScore(10);
//			currentlyProcessedPost.setOldPostScore(10);
			currentlyProcessedPost.getCommenters().add(currentlyProcessedPost.getUser_id()); // put the post author inside
			
			/*
			 * update the ring structure: insert a new entry
			 */
			TimeTableEntry newEntry = new TimeTableEntry(newSimulationTime, currentlyProcessedPost);
			if (this.lastEntry != null){
				this.lastEntry.addNewSuccessor(newEntry);
				this.lastEntry = newEntry;
			}
			else{
				this.lastEntry = newEntry;
				this.lastEntry.setSuccessor(this.lastEntry);
			}
			/*
			 * any case: changed posts: add the new post
			 */
			if (currentlyProcessedPost.isUpdateThisRound() == false){
				result.add(currentlyProcessedPost);
				currentlyProcessedPost.setUpdateThisRound(true);
			}
									
		}
		if (currentlyProcessedComment != null){
			
			/*
			 * update the ring structure: insert a new entry
			 */
			final Post commentedPost = currentlyProcessedComment.getPost(); 
			
			/*
			 * if post had already been dead when its comment was streamed, it is null
			 */
			if (commentedPost != null && commentedPost.getPostScore() > 0){
				TimeTableEntry newEntry = new TimeTableEntry(newSimulationTime, commentedPost);
				newEntry.setCommenter_user_id(currentlyProcessedComment.getUser_id());
				if (this.lastEntry != null){
					this.lastEntry.addNewSuccessor(newEntry);
					this.lastEntry = newEntry;
				}
				else{
					this.lastEntry = newEntry;
					this.lastEntry.setSuccessor(this.lastEntry);
				}
				
				/*
				 * increment the postscore's totalscore by 10
				 */
				commentedPost.incrementPostScore(10);
				
				/*
				 * add the commenter to the commenter's list of post
				 */
				commentedPost.getCommenters().add(currentlyProcessedComment.getUser_id());
				
				/*
				 * any case: changed posts: add the post
				 */
				if (commentedPost.isUpdateThisRound() == false){
					result.add(commentedPost);
					commentedPost.setUpdateThisRound(true);
				}
			}
		}
		
		return result;
	}

		
	/**
	 * 
	 * @param comment
	 * @return the updates yielded by processing the comment
	 */
	private LinkedList<Post> handleComment(Comment comment){
		final long commentId = comment.getComment_id();
		final long postId = comment.getPost_commented();
		/*
		 * set up the comment link
		 */
		this.commentId2Comment.put(comment.getComment_id(), comment);
		
		/*
		 * // the comment is directly associated to a post
		 */
		if (postId != -1){ 
			final Post post = this.postId2Post.get(postId); 
			
			if (post != null){ // post is null if post has been deleted in the meanwhile when becoming inactive
				
				/*
				 * link comment-->post
				 */
				comment.setPost(post);
				
				/*
				 * back-link post-->comment
				 */
				post.getComments().add(commentId);
				
				/*
				 * comments timestamps
				 */
				post.getCommentTimestamps().add(comment.getTs());
			}
						
		}
		/*
		 *  the comment is a reply to another comment
		 */
		else {
			Post post = this.commentId2Comment.get(comment.getComment_replied()).getPost();// which post the comment replied to is associated with
							
			if (post != null){ // postInfo is null when post has been deleted in the meanwhile due to becoming inactive
				
				comment.setPost(post);	// save the direct link to a comment's commented post
					
				/*
				 * back-link post-->comment
				 */
				post.getComments().add(commentId);
					
				/*
				 * comments timestamps
				 */
				post.getCommentTimestamps().add(comment.getTs());
					
				/*
				 * commenters set
				 */
				post.getCommenters().add(comment.getUser_id());
			}
			
		}
		
		// progress
		return this.updateSimulationTime(comment.getTs(), null, comment);
	}
	
	private LinkedList<Post> handlePost(Post post){
		
		
		/*
		 * set up the PostInformation
		 */
		this.postId2Post.put(post.getPost_id(), post);
				
		// progress
		return this.updateSimulationTime(post.getTs(), post, null);	
		
	}
	
	
	
	/*
	 * compute the top3 posts
	 */
	/**
	 * compare two posts: can the first post take the position of the second post in a ranking?
	 * @param first first post 
	 * @param second second post 
	 * @return 1 if first post is "higher", -1 if second post is "higher"; 0 if they are the same
	 */
	public int comparePosts(Post first, Post second){
		
		if (first.equals(second)){
			return 0;
		}
	
		long timeStampFirst = first.getTs();
		long timeStampSecond = second.getTs();
		if (timeStampFirst > timeStampSecond){
			return 1;
		}
		else if (timeStampFirst < timeStampSecond){
			return -1;
		}
		else { // timeStampFirst == timeStampSecond
			// a draw: now compare the timestamps of the comments: latest comment first
			
			LinkedList<Long> commentTimestampsFirst = first.getCommentTimestamps();
			LinkedList<Long> commentTimestampsSecond = second.getCommentTimestamps();
			Iterator<Long> iteratorFirst = commentTimestampsFirst.descendingIterator();
			Iterator<Long> iteratorSecond = commentTimestampsSecond.descendingIterator();
			while(iteratorFirst.hasNext()){
				long firstPostCommentTs = iteratorFirst.next();
				if (iteratorSecond.hasNext()){
					long secondPostCommentTs = iteratorSecond.next();
					if (firstPostCommentTs > secondPostCommentTs){
						return 1;
					}
					else if (firstPostCommentTs < secondPostCommentTs){
						return -1;
					}
					// else: there is still a tie; check the next event; proceed in the tiebreaker loop
				}
				else { // the second post has no more comments; the first post wins
					return -1;
				}
			}
			// the first post does not have more comments; thus, it loses the comparison
			return 1;
		}
	}
	
	private void adaptTH(int newTH){
//		System.out.println("adapt");
		/*
		 * if they are the same, do nothing...
		 */
		if (newTH == this.sortingScoreTH){
			return;
		}
		else {
			// adapt the th
			/*
			 * two cases: 
			 * 
			 * a) newTH > oldTH 
			 *    In this case, post from the sorted list go to the unsorted list, because the threshold to stay is the
			 *    sorted list has become "stricter"
			 *    
			 * b) newTH < oldTH
			 * 	  In this case, posts from the unsorted list go to the sorted list, because the threshold to go to the
			 * 	sorted list has become more "relaxed"
			 */
			if (newTH > this.sortingScoreTH){
				Iterator<Post> sit = this.sortedPosts.iterator();
				while (sit.hasNext()){
					Post p = sit.next();
					// post goes to unsorted list:
					if (p.getOldPostScore() <= newTH){ 
						// post leaves the sorted list
						sit.remove();
						// post goes to unsorted list
						this.unsortedPosts.add(p);
						p.setInUnsortedSet(true);
					}
					else {
						break; // stop here, the first post is reached that stays in the sorted list
					}
					
				}
			}
			else {
				// case b: newTH < OldTH
				Iterator<Post> uit = this.unsortedPosts.iterator();
				while (uit.hasNext()){
					Post p = uit.next();
					if (p.getOldPostScore() > newTH){
						// post leaves the unsorted list
						uit.remove();
						p.setInUnsortedSet(false);
						// post is sorted into the sorted list
						this.sortedPosts.add(p);
					}
					// else: the post stays in the unsorted list; it cannot make it beyond the relaxed threshold
				}
			}
			this.sortingScoreTH = newTH;
			// done.
		}
		
		
	}
	
	
	/**
	 * computes the new top 3 and produces an output if something has changed
	 * @param updates
	 */
	private void computeNewTop3(LinkedList<Post> updates, long timestamp, long creationtime){
		boolean top3changed = false;
		
		/*
		 * empty updates can occur at the end of a stream
		 */
		if (updates.size() == 0){
			return;
		}
		
//		/*
//		 * only one post was updated
//		 */
//		else if (updates.size() == 1){
//			
//			for (Post updatedPost : updates){
//				
//				// post is dead
//				if (updatedPost.getPostScore() <= 0){
//					this.unsortedPosts.remove(updatedPost);
//					this.sortedPosts.remove(updatedPost);
//					
//					// check if the dead post has been in top3 (most likely, it hasn't)
//					boolean deadPostWasInTop3 = false;
//					for (int i = 0; i < 3; i++){
//						if (top3[i][1] == updatedPost.getPost_id()){
//							deadPostWasInTop3 = true;
//							top3changed = true;
//							break;
//						}
//					}
//					if (!deadPostWasInTop3){ // if it hasn't been in top3, the top3 do not change by the death of the post
//						return;
//					}
//				}
//			}
//		}
		
		/*
		 * reorder the updated post
		 */
		for (Post updatedPost : updates){
			 
//			this.sortedPosts.remove(updatedPost);
			
			/*
			 * if the new score is <= 0 (post is dead), remove it from all data structured withour re-adding it anywhere
			 */
			if (updatedPost.getPostScore() <= 0){
				
				for (int i = 0; i < 3; i++){
					if (top3[i][1] == updatedPost.getPost_id()){
						top3changed = true;
						break;
					}
				}
				
				if (updatedPost.getOldPostScore() <= this.sortingScoreTH){
					this.unsortedPosts.remove(updatedPost);
				}
				else {
					this.sortedPosts.remove(updatedPost);
				}
				continue; // post is not further processed; it is dead and hence, not considered for top3.
			}
			
			// post HAS BEEN in UNSORTED list before
			if (updatedPost.getOldPostScore() <= this.sortingScoreTH){
				// post goes to sorted list:
				if (updatedPost.getPostScore() > this.sortingScoreTH){ 
					// post leaves the unsorted list
					this.unsortedPosts.remove(updatedPost);
					updatedPost.setInUnsortedSet(false);
					// post gets new post score
					updatedPost.setOldPostScore(updatedPost.getPostScore());
					// post gets sorted into the sorted list
					this.sortedPosts.add(updatedPost);
				}
				// post goes to unsorted list if not already there
				else {
					// post gets a new post score
					updatedPost.setOldPostScore(updatedPost.getPostScore());
					// post gets added to unsorted list
					if (!updatedPost.isInUnsortedSet()){
						this.unsortedPosts.add(updatedPost);
					}
				}
			}
			// post has been in sorted list before
			else {
				this.sortedPosts.remove(updatedPost);
				// post gets new post score
				updatedPost.setOldPostScore(updatedPost.getPostScore());
				if (updatedPost.getPostScore() > this.sortingScoreTH){ 
					// post gets sorted into the sorted list
					this.sortedPosts.add(updatedPost);
				}
				else {
					// post gets added to unsorted list
					this.unsortedPosts.add(updatedPost);
					updatedPost.setInUnsortedSet(true);
				}
			}
			
			updatedPost.setUpdateThisRound(false);
			
		}
		
					
		/*
		 * update top3
		 *
		 */
		Iterator<Post> sortedIterator = this.sortedPosts.descendingIterator();
		boolean top3done = false; // top3 have been set
		boolean sorteddone = false; // sorted posts have all been checked

		Post setPost = null; // currently considered the post on this rank
		Post candidatePost = null; // checked whether this one takes its rank
		boolean candidatePostReadByIterator = false; // indicate the candidate post has already been taken by the iterator
		int i = 0;
		
		if (sortedIterator.hasNext()){
			setPost = sortedIterator.next();
		}
		else { // there is no post 
			sorteddone = true;
		}
		/*
		 * iterate through the sorted posts
		 */
		sortedloop:
		while (!top3done && !sorteddone){

			if (!sortedIterator.hasNext()){
			// there are no more posts in the sorted list
				
				if (!candidatePostReadByIterator){					
					// set post is in rank i and the ranking is finished
					if (top3changed == false && top3[i][1] != setPost.getPost_id()){
						top3changed = true;
					}
					top3[i][0] = setPost.getPostScore();
					top3[i][1] = setPost.getPost_id();
//							System.out.println("place A: setting position " + i + " to post " + top3[i][1] + " score " + top3[i][0]);
					if (top3[i][0] <= 0){
						top3changed = true;
					}
				}
				else { // also read the candidate post, even if there is no other post left;
					// set post is in rank i and the ranking is finished
					if (top3changed == false && top3[i][1] != candidatePost.getPost_id()){
						top3changed = true;
					}
					top3[i][0] = candidatePost.getPostScore();
					top3[i][1] = candidatePost.getPost_id();
//							System.out.println("place B: setting position " + i + " to post " + top3[i][1] + " score " + top3[i][0]);
					if (top3[i][0] <= 0){
						top3changed = true;
					}
					
				}
				i++;
				sorteddone = true;
				break sortedloop;
			}
			// there are more posts in the sorted iterator
			else {
				if (candidatePostReadByIterator){
					// candidate post is next set post
					setPost = candidatePost;
				}
				
				candidatePost = sortedIterator.next();
				candidatePostReadByIterator = true;
				
				/*
				 * check whether the candidate post has the same score and timestamp as the set post
				 * 
				 * case 1: the scores or timestamp are different; the set post is the next post
				 */
				if (candidatePost.getPostScore() != setPost.getPostScore() || candidatePost.getTs() != setPost.getTs()){
					// candidate has different, lower score, or different, lower timestamp; set post is the winner for rank i
					if (top3changed == false && top3[i][1] != setPost.getPost_id()){
						top3changed = true;
					}
					top3[i][0] = setPost.getPostScore();
					top3[i][1] = setPost.getPost_id();
//					System.out.println("place C: setting position " + i + " to post " + top3[i][1]);
					if (top3[i][0] <= 0){
						top3changed = true;
					}
					i++;
					if (i > 2){
						top3done = true;
						break sortedloop;
					}
				}
				/*
				 * case 2: the score and timestamp are the same;
				 * find further candidate posts in the candidate list
				 */
				else {
					
					LinkedList<Post> candidateList = new LinkedList<Post>();
					candidateList.add(setPost);
					candidateList.add(candidatePost);
					candidatebuildloop:
					while (true){
						if (sortedIterator.hasNext()){
							
							candidatePost = sortedIterator.next();
							if (candidatePost.getPostScore() == setPost.getPostScore() && candidatePost.getTs() == setPost.getTs()){
								candidateList.add(candidatePost);
	//							System.out.println("added post to candidate list " + candidatePost.getPost_id());
							}
							else { // a post with other score or ts is reached
								break candidatebuildloop;
							}
						}
						else { // no more posts are there
							candidatePostReadByIterator = false;
							sorteddone = true; // no more posts in sorted list
							break candidatebuildloop;
						}
					}
					// candidate list is complete
					// sort it
					candidateList.sort(new Comparator<Post>(){
							@Override
						public int compare(Post post0, Post post1) {
							return comparePosts(post0, post1);
						}
						
					});
					
					// put the sorted values into the top3 list
					candidateinsertionloop:
					for (Post next : candidateList){
						if (top3changed == false && top3[i][1] != next.getPost_id()){
							top3changed = true;
						}
						top3[i][0] = next.getPostScore();
						top3[i][1] = next.getPost_id();
						if (top3[i][0] <= 0){
							top3changed = true;
						}
//						System.out.println("place D: setting position " + i + " to post " + top3[i][1]);
						i++;
						if (i > 2){
							top3done = true;
							break candidateinsertionloop;
						}
					}
				}
				
			}
		}
		// end of sortedloop
		
		/*
		 * unsortedeloop works as follows:
		 * beginning from the current index i, we try to find the highest posts in all entries;
		 * that means, we have to find the top 3 - i entries
		 */
		if (i < 3 && unsortedPosts.size() > 0){
//			System.out.println("tree miss " + unsortedPosts.size());
				TreeSet<Post> temporarySortedUnsorted = new TreeSet<Post>(
						new Comparator<Post>(){
							@Override
								public int compare(Post post0, Post post1) {
									return comparePostsComplete(post0, post1);
								}
						});
				
				// sort the unsorted posts by putting them into the temporary treeset
				for (Post p : unsortedPosts){
					temporarySortedUnsorted.add(p); // now they are sorted
				}
				
				Iterator<Post> sortedunsortedit = temporarySortedUnsorted.descendingIterator();
				// now fill the rest of top3
				while (i < 3){
					if (sortedunsortedit.hasNext()){
						Post next = sortedunsortedit.next();
						if (top3changed == false && top3[i][1] != next.getPost_id()){
							top3changed = true;
						}
						top3[i][0] = next.getPostScore();
						top3[i][1] = next.getPost_id();
						if (top3[i][0] <= 0){
							top3changed = true;
						}
						i++;
					}
					else {
						break;
					}
				}
			}
			
		if (i < 3){
			// still, i is not filled; fill the rest with 0s
			for (; i < 3; i++){
				if (top3[i][0] != 0){
					top3changed = true;
				}
				top3[i][0] = 0;
				top3[i][1] = 0;
			}
		}
//		System.out.println(sortedPosts.size());
//		System.out.println(unsortedPosts.size());
//		System.out.println();
		/*
		 * if top3 have changed, create an output
		 */
		if (top3changed){
			this.writeOutNewTop3(timestamp, creationtime, top3);
		}
		
		
		/*
		 * adapting the TH for sorted / non-sorted post list
		 */
		if ( (top3[2][0] - this.sortingScoreTH) <= 20 ){ // 20 is the lower bound of difference
			this.adaptTH(this.sortingScoreTH - 5); // 10 is the adaptation factor
		}
		else if ((top3[2][0] - this.sortingScoreTH) >= 40 ){ // 40 is the upper bound of difference
			this.adaptTH(this.sortingScoreTH + 5); // 10 is the adaptation factor
		}
		
//		System.out.println(this.sortedPosts.size());
//		System.out.println(this.unsortedPosts.size());
//		System.out.println();
		
	}
	
	/**
	 * compare posts complete with score, timestamp and comments
	 * @param post1 post 1
	 * @param post2 post 2
	 * @return  1 if first post is "higher", -1 if second post is "higher"; 0 if they are the same
	 */
	private int comparePostsComplete(Post post1, Post post2){
		if (post1.getPostScore() == post2.getPostScore()){
			return this.comparePosts(post1, post2);
		}
		else {
			if (post1.getPostScore() > post2.getPostScore()){
				return 1;
			}
			else {
				return -1;
			}
		}
	}

	/**
	 * writes the new top 3 to file
	 * @param timestamp timestamp of the update, for logging
	 * @param creationTime creation time of updating post or comment for sake of time measurement
	 */
	private void writeOutNewTop3(long timestamp, long creationTime, long[][] newTop3){
		// output the top 3, timestamp of the current step.
		try {
			StringBuilder outstring = new StringBuilder();
			outstring.append(timeParser.timeStamp(timestamp));
			for (int i = 0; i <=2; i++){
				if (newTop3[i][1] == 0){
					outstring.append(",-,-,-,-"); // no post_id, no post_user, no post_score, no post_commenters
					continue; // there is no entry for this position
				}
				Post thepost = this.postId2Post.get(newTop3[i][1]);
				if (thepost != null){
					outstring.append(',');
					outstring.append(newTop3[i][1]);
					outstring.append(',');
					outstring.append(thepost.getUser());
					outstring.append(',');
					outstring.append(thepost.getPostScore());
					outstring.append(',');
					outstring.append(thepost.getCommenters().size() - 1);
				}
				else { // post doesn't exist any more, it is dead.
					outstring.append(",-,-,-,-");
				}
			}
			outstring.append(System.lineSeparator());
						
			/*
			 *  latency measurement
			 */
			numberOfEvents++;
			long currentEventLatency = System.nanoTime() - creationTime; 
			latencyPerEventSummed += currentEventLatency;
			double averageLatencyQ1 = (double)latencyPerEventSummed/(double)numberOfEvents;
			eventLatencyLogger.log(System.nanoTime() + "\t" + currentEventLatency + "\n");
			this.out.write(outstring.toString());
						
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		boolean finished = false;
		AbstractEvent nextEvent = null;
		
		runloop:
		while (true){
			/*
			 * assign the post or comment to the worker which is responsible for it
			 */
//				AbstractEvent nextEvent = Serializer.query1Queue.take();
			/**
			 * events are read in a batch; then the batch is processed
			 */
						
//			try {
//				nextEvent = Serializer.query1Queue.take();
				nextEvent = serializer.getNextEvent();
//			} catch (InterruptedException e1) {
//				e1.printStackTrace();
//			}
			
//			if (nextEvent != null){
				
				LinkedList<Post> update = null;
				switch (nextEvent.getType()){
				case COMMENT:
					update = this.handleComment((Comment) nextEvent);
					break;
				case POST:
					update = this.handlePost((Post) nextEvent);
					break;
				case STREAM_CLOSED: 
					finished = true;
					break; // the splitter will shut down
				default:
					//////System.out.println("neither a post nor a comment in query 1"); System.exit(1);
					break;		
				}
				
				// process the update: updating top3 if applicable
				if (finished){
					
					/*
					 * at the end, make a last simulation step to the "end of all times" to flush out all pending updates / post deaths
					 */
					this.updateSimulationTime(Long.MAX_VALUE, null, null); 
					
					// shut down
					try {
						/*
						 * Close writers and write latency statistics to Statistics module.
						 */
						this.out.close();
						long totalTime = (System.currentTimeMillis()-ManagementMain.getStartProcessingTime());
						double averageLatencyQ1 = (double)latencyPerEventSummed/(double)numberOfEvents;
						Statistics.setDurationQ1(totalTime);
						Statistics.setAverageLatencyQ1(averageLatencyQ1);
						break runloop;
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				else{
					// update top3
					this.computeNewTop3(update, nextEvent.getTs(), nextEvent.getCreationTime());
				}
				
//			}
			
		}
					
	}
	
	
}
