package com.vf.threadpool;

import java.util.Random;

public class Main {

	private static final String[] jobIds = new String[]{"FileJob","DBJob","NetworkJob"};
	
	
	public static void main(String[] args) {
		
		Random random = new Random();
		JobAffinityThreadPool pool = new JobAffinityThreadPool();
		int count = 1;
		
		for (int i = 0; i < 20; i++) {
			String jobId = jobIds[random.nextInt(3)];
			WorkerThread worker = new WorkerThread(jobId, count);
			
			pool.submit(jobId, worker);
			count++;
		}
		
		System.out.println("Sending shutdown request to job affinity thread pool");
		pool.shutdown();
	}
}

