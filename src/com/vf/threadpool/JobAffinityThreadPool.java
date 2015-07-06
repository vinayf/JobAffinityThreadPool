package com.vf.threadpool;

import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * A Thread Pool with job affinity, currently this pool will have as many threads 
 * as many number of unique job ids submitted to the pool which may not be a good idea.
 * 
 * TODO this can be enhanced to a fixed thread pool with mechanisms to either re-utilize or
 * kill and re-create new threads based on the use case once a set of job ids have been processed.
 * 
 * @author vinayf
 * @date   06-Jul-2015
 */
public class JobAffinityThreadPool implements ThreadPoolWithJobAffinity {

	public Map<String, Queue<Runnable>> jobQueueMap = new Hashtable<String, Queue<Runnable>>();
	public Map<String, Runnable> affinityMapping = new Hashtable<String, Runnable>();
	public volatile boolean shutdown = false;
	
	@Override
	public void submit(String jobId, Runnable job){
		if(jobQueueMap.containsKey(jobId)){
			jobQueueMap.get(jobId).add(job);
			synchronized (jobQueueMap) {
				jobQueueMap.notifyAll();
			}
		}else{
			Queue<Runnable> queue = new LinkedList<Runnable>();
			queue.add(job);
			jobQueueMap.put(jobId, queue);
			Thread t = new Thread(new PoolThread(jobId, this));
			affinityMapping.put(jobId, t);
			t.setName(jobId+"-Thread");
			t.start();
		}
	}
	
	@Override
	public void shutdown(){
		shutdown = true;
	}
	
	@Override
	public int poolSize(){
		return affinityMapping.size();
	}
}

/**
 * A Thread part of the affinity thread pool which based on the affinity
 * pulls the tasks from the queue and runs the worker threads.
 * @author vinayf
 * @date   06-Jul-2015
 */
class PoolThread implements Runnable{

	private ThreadLocal<String> threadLocal = null;
	private JobAffinityThreadPool pool;
	
	public PoolThread(final String jobId, JobAffinityThreadPool pool) {
		this.pool = pool;
		
		threadLocal = new ThreadLocal<String>() {
			@Override
			protected String initialValue() {
				return jobId;
			}
		};
		
	}
	
	@Override
	public void run() {
		while(true){
			
			if(pool.jobQueueMap.get(threadLocal.get()).size()>0){
				Runnable job = pool.jobQueueMap.get(threadLocal.get()).poll();
				job.run();
			}else{
				if(pool.shutdown)
					break;
				while(pool.jobQueueMap.get(threadLocal.get()).size()==0){
					synchronized (pool.jobQueueMap) {
						try {
							pool.jobQueueMap.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
			
		}
		System.out.println("Pool Thread for Job Id >> "+threadLocal.get()+", exiting.");
	}
}

/**
 * A WorkerThread which processes the submitted job.
 * @author vinayf
 * @date   06-Jul-2015
 */
class WorkerThread implements Runnable{

	String jobId;
	int number;
	public WorkerThread(String jobId, int number) {
		this.jobId = jobId;
		this.number = number;
	}
	
	@Override
	public void run() {
		System.out.println(Thread.currentThread().getName()+" - processing >> "+jobId+" - "+number);
	}
	
}
