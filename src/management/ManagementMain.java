package management;

import common.events.EventType;
import query1.Query1Manager;
import query1.Query1SingleThreaded;
import query2.Query2Manager;
import util.Constants;

/**
 * The main method
 * 
 * starts all the necessary threads:
 * 
 * - Source Connectors for each of the 4 source streams
 * - Serializer putting the source streams into the 2 incoming streams of the two queries
 * - QueryProcessors
 * @author mayerrn
 *
 */
public class ManagementMain {
	
	private static long startProcessingTime;
	private static SerializerQ1 serializerQ1;
	private static SerializerQ2 serializerQ2;
	

	public static void main(String[] args) {
				
		/*
		 * start global time measuring 
		 */
		startProcessingTime = System.currentTimeMillis();
		
		if (args.length==6) {
			
			// load file names from parameter
			String friendshipFile = args[0];
			String postsFile = args[1];
			String commentsFile = args[2];
			String likesFile = args[3];
			Constants.Q2_K = Integer.valueOf(args[4]);
			Constants.Q2_D = Integer.valueOf(args[5]);
			
			serializerQ1 = new SerializerQ1(commentsFile, friendshipFile, likesFile, postsFile);
			serializerQ2 = new SerializerQ2(commentsFile, friendshipFile, likesFile, postsFile);
			
			// start the serializer
//			Thread serializerThread = new Thread(new Serializer(commentsFile, friendshipFile, likesFile, postsFile));
//			serializerThread.start();
			
			// start the 4 SourceConnectors
//			Thread commentConnector = new Thread(new SourceConnector(EventType.COMMENT, commentsFile));
//			commentConnector.setPriority(Thread.MIN_PRIORITY);
//			commentConnector.start();
//			Thread friendshipConnector = new Thread(new SourceConnector(EventType.FRIENDSHIP, friendshipFile));
//			friendshipConnector.setPriority(Thread.MIN_PRIORITY);
//			friendshipConnector.start();
//			Thread likeConnector = new Thread(new SourceConnector(EventType.LIKE, likesFile));
//			likeConnector.setPriority(Thread.MIN_PRIORITY);
//			likeConnector.start();
//			Thread postConnector = new Thread(new SourceConnector(EventType.POST, postsFile));
//			postConnector.setPriority(Thread.MIN_PRIORITY);
//			postConnector.start();

		} else {
			System.err.println("Six input parameters expected!");
			System.err.println("Input params: " + args);
			System.exit(-1);
		}
		
		// start Query1 execution
////		Query1Manager.startExecution();
		Thread q1Thread = new Thread(new Query1SingleThreaded(serializerQ1));
////		q1Thread.setPriority(Thread.MAX_PRIORITY);
		q1Thread.start();
		
		// start Query2 execution
		
		Thread q2Thread = new Query2Manager(serializerQ2);
//		q2Thread.setPriority(Thread.MAX_PRIORITY);
		q2Thread.start();
	}


	public static long getStartProcessingTime() {
		return startProcessingTime;
	}


}
