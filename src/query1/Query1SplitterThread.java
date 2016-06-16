package query1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import common.events.AbstractEvent;
import common.events.Comment;
import common.events.Marker;
import common.events.Post;
import common.events.PostInactive;
import management.Serializer;
import util.Constants;

/**
 * 
 * takes posts and comments from the query1 inqueue
 * 
 * assigns the posts and comments to the Query1WorkerThreads according to their post_id
 * 
 * 
 * @author mayerrn
 *
 */
public class Query1SplitterThread implements Runnable {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
//	
//	
//	/**
//	 * comment_id --> post_id it is a comment to
//	 * 
//	 * this datastructure is needed in order to associate comments replying to other comments to a post
//	 * 
//	 * new comments are inserted into the list associated to their post
//	 * 
//	 * upon deletion of a post, all the associated comments are deleted
//	 */
//	private HashMap<Long,Long> comment2Post = new HashMap<Long,Long>(100000);
//	
//	/**
//	 * post_id --> post
//	 * 
//	 * also used by the worker threads to access the post information when needed
//	 */
//	private ConcurrentHashMap<Long, Post> postInfos = new ConcurrentHashMap<Long, Post>(100000);
//	
//	/**
//	 * notifications of inactive posts
//	 * 
//	 * those posts are deleted from the internal data structures by the Splitter
//	 */
//	private LinkedBlockingQueue<PostInactive> inactivePostNotifications = new LinkedBlockingQueue<PostInactive>();
//		
//	/**
//	 * add the author's user_id to the commenters set of the post
//	 * 
//	 * assign post to worker thread according to the post_id
//	 * @param post the post to be assigned
//	 * @return the workerid the post was assigned to
//	 */
//	private int handlePost(Post post){
//		final long postid = post.getPost_id();
//		
//		/*
//		 * set up the PostInformation
//		 */
//		postInfos.put(postid, post);
//				
//		/*
//		 * assignment to worker thread
//		 */
//		int workerId = (int) (postid % Constants.Q1_NUM_WORKERS);
//		try {
//			Query1Manager.getWorkers().get(workerId).getInQueue().put(post);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		
//		return workerId;
//	}
//	
//	/**
//	 * check which post is associated and assign the comment to the associated worker thread
//	 * 
//	 * add the author's user_id to the commenters set of the post
//	 * @param comment the comment to be assigned
//	 * @return the worker id the comment was assigned to
//	 */
//	private int handleComment(Comment comment){
//		final long commentId = comment.getComment_id();
//		long postId = comment.getPost_commented();
//		
//		/*
//		 * // the comment is directly associated to a post
//		 */
//		if (postId != -1){ 
//			Post post = this.postInfos.get(postId);
//			if (post != null){ // post is null if post has been deleted in the meanwhile when becoming inactive
////				System.out.println("comment with post");
//				
//				/*
//				 * link comment-->post
//				 */
//				this.comment2Post.put(commentId, postId);
//				
//
//				/*
//				 * back-link post-->comment
//				 */
//				post.getComments().add(commentId);
//				
//				/*
//				 * comments timestamps
//				 */
//				post.getCommentTimestamps().add(comment.getTs());
//
//			}
//						
//		}
//		/*
//		 *  the comment is a reply to another comment
//		 */
//		else {
//			postId = this.comment2Post.get(comment.getComment_replied()); // which post the comment replied to is associated with
//			Post post = this.postInfos.get(postId);
//				
//			if (post != null){ // postInfo is null when post has been deleted in the meanwhile due to becoming inactive
//				
//				comment.setPost_commented(postId); // save that information in the comment itself for easy processing in worker
//									
//				/*
//				 * link comment-->post
//				 */
//				this.comment2Post.put(commentId, postId);
//					
//				/*
//				 * back-link post-->comment
//				 */
//				post.getComments().add(commentId);
//					
//				/*
//				 * comments timestamps
//				 */
//				post.getCommentTimestamps().add(comment.getTs());
//					
//				/*
//				 * commenters set
//				 */
//				post.getCommenters().add(comment.getUser_id());
//			}
//			
//		}
//		
//		/*
//		 * associate the comment to the worker thread that handles the post the comment is associated with
//		 */
//		int workerId = (int) (postId % Constants.Q1_NUM_WORKERS);
//		try {
//			Query1Manager.getWorkers().get(workerId).getInQueue().put(comment);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		return workerId;
//	}
//
//	@Override
//	public void run() {
//		
//		runloop:
//		while (true){
//			try {
//				/*
//				 * assign the post or comment to the worker which is responsible for it
//				 */
//				AbstractEvent nextEvent = Serializer.query1Queue.take();
//				PostInactive postInactiveNotification = this.inactivePostNotifications.poll();
//				/*
//				 * if there is currently an inactive post, process that first
//				 */
//				if (postInactiveNotification != null){
//					/*
//					 * delete from comment2post and postInfos
//					 */
//					ArrayList<Long> comment_idsOfInactivePost = this.postInfos.get(postInactiveNotification.getPost_id()).getComments();
//					this.postInfos.remove(postInactiveNotification.getPost_id()); 
//					for (long i : comment_idsOfInactivePost){
//						this.comment2Post.remove(i);
//					}
//				}
//				
//				int workerId = -1;
//				switch (nextEvent.getType()){
//				case COMMENT:
//					workerId = this.handleComment((Comment) nextEvent);
//					break;
//				case POST:
//					workerId = this.handlePost((Post) nextEvent);
//					break;
//				case STREAM_CLOSED: 
//					for (int i = 0; i < Constants.Q1_NUM_WORKERS; i++){					
//						Query1Manager.getWorkers().get(i).getInQueue().put(nextEvent); // forward the stream_closed event to all Q1 workers
//					}
//					break runloop; // the splitter will shut down
//				default:
//					System.out.println("neither a post nor a comment in query 1"); System.exit(1);
//					break;		
//				}
//				
//
//				/*
//				 * pass a marker message (timestamp) to each other worker to trigger the processing progress
//				 * not sent to the worker the post or comment was assigned to
//				 */
//				Marker marker = new Marker(nextEvent.getTs(), nextEvent.getCreationTime());
//				for (int i = 0; i < Constants.Q1_NUM_WORKERS; i++){
//					if (i != workerId){
//						Query1Manager.getWorkers().get(i).getInQueue().put(marker);
//					}
//				}
//				
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//		
////		System.out.println("Query1Splitter shut down.");
//	}
//
//	public HashMap<Long, Long> getComment2Post() {
//		return comment2Post;
//	}
//
//	public void setComment2Post(HashMap<Long, Long> comment2Post) {
//		this.comment2Post = comment2Post;
//	}
//
//	public ConcurrentHashMap<Long, Post> getPostInfos() {
//		return postInfos;
//	}
//
//	public void setPostInfos(ConcurrentHashMap<Long, Post> postInfos) {
//		this.postInfos = postInfos;
//	}
//
//	public LinkedBlockingQueue<PostInactive> getInactivePostNotifications() {
//		return inactivePostNotifications;
//	}
//
//	public void setInactivePostNotifications(LinkedBlockingQueue<PostInactive> inactivePostNotifications) {
//		this.inactivePostNotifications = inactivePostNotifications;
//	}

	
}
