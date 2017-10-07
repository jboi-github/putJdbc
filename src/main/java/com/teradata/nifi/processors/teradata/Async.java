/**
 * 
 */
package com.teradata.nifi.processors.teradata;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Run tasks asynchronously. It can either run on all available cores of the JVM or on one thread,
 * which effectively runs all tasks serially. The class must be overwritten and one or more methods
 * should be added, that will be executed asynchronously.
 * An example method is implemented to explain the idea.
 * 
 * @author juergenb
 *
 */
public class Async {
	private ExecutorService executorService;
	private Queue<Future<Void>> futures;
	
	/**
	 * @param threadsToLeave: Number of threads to not use from available processors.
	 * 		If number is higher or equal to number of available processors, then the executor is single threaded.
	 * @param hintOnBlocksOfTasks: submitted tasks are stored to check for results and exceptions later.
	 * 		The list to store these tasks is extended by the given number or 10, if number is <= 0.
	 */
	public Async(int threadsToLeave) {
		int nThreads = Runtime.getRuntime().availableProcessors() - threadsToLeave;
		executorService = (nThreads > 0)? Executors.newFixedThreadPool(nThreads) : Executors.newSingleThreadExecutor();
		futures = new ConcurrentLinkedQueue<Future<Void>>();
	}
	
	/**
	 * Shutdown Executor, so that no new tasks can be submitted. Then wait for each task to end, re-throw the Exception of the first
	 * Task hitting an exception.
	 * Waiting is done like this:
	 * 1. Do a friendly shutdown and await terminations.
	 * 2. Get result of all tasks and collect first exception if it exists. Get all results regardless of thrown Exceptions.
	 * 3. Re-Throw first Exception, if it exists.
	 * 
	 * @throws ExecutionException
	 * @throws InterruptedException 
	 */
	public void syncAndEnd() throws ExecutionException, InterruptedException {
		// Shutdown friendly. Await termination for ever
		executorService.shutdown();
		while(!executorService.isTerminated()) {
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
		}

		// Get results and collect first Exception
		ExecutionException executionException = null;
		for(Future<Void> future : futures) {
			try {
				future.get();
			} catch (ExecutionException e) {
				if(executionException == null) executionException = e;
			}
		}
		
		// re-throw exception, if one exists
		if(executionException != null) throw executionException;
	}

	/**
	 * Currently best reaction on interrupt exceptions. shutdown now to execution and interrupt current thread. 
	 */
	public void interrupt() {
		executorService.shutdownNow();
		Thread.currentThread().interrupt();
	}
	
	/**
	 * Submit a task. This method will be called by an overwriting class, that implements a more specific task method.
	 * @param callable is the task to be called, that might throw an exception during runtime.
	 */
	protected void submit(Callable<Void> callable) {
		futures.add(executorService.submit(callable));
	}
	
	private void doSomething(final int number, final String text) {
		submit(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				System.out.println(text + ", " + number);
				return null;
			}}
		);
	}
	
	/**
	 * @param args
	 * @throws ExecutionException 
	 */
	public static void main(String[] args) throws ExecutionException {
		Async async = new Async(0);
		for(int i = 0; i < 1000; i++) async.doSomething(i, "i = ");
		try {
			async.syncAndEnd();
		} catch (InterruptedException e) {
			System.out.println("Interrupted!");
			async.interrupt();
		}
	}
}
