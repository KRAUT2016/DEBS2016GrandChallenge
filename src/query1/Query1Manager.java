package query1;

import java.util.ArrayList;

import util.Constants;

/**
 * starts the Query1WorkerThreads and Query1MergerThread and keeps references to them
 * 
 * @author mayerrn
 *
 */
public class Query1Manager {
	
	private static ArrayList<Query1WorkerThread> workers = new ArrayList<Query1WorkerThread>();
	
	private static Query1SplitterThread splitter;
	
	private static Query1MergerThread merger;
	
	/**
	 * starts all the threads needed in order to process query1
	 */
	public static void startExecution(){

		splitter = new Query1SplitterThread();
		new Thread(splitter).start();
		
		merger = new Query1MergerThread();
		new Thread(merger).start();
		
		for (int i = 0; i < Constants.Q1_NUM_WORKERS; i++){
			Query1WorkerThread worker = new Query1WorkerThread();
			workers.add(worker);
			new Thread(worker).start();
		}
		
		
	}
		

	public static ArrayList<Query1WorkerThread> getWorkers() {
		return workers;
	}

	public static void setWorkers(ArrayList<Query1WorkerThread> workers) {
		Query1Manager.workers = workers;
	}
	
	public static Query1SplitterThread getSplitter() {
		return splitter;
	}

	public static void setSplitter(Query1SplitterThread splitter) {
		Query1Manager.splitter = splitter;
	}

	public static Query1MergerThread getMerger() {
		return merger;
	}

	public static void setMerger(Query1MergerThread merger) {
		Query1Manager.merger = merger;
	}

}
