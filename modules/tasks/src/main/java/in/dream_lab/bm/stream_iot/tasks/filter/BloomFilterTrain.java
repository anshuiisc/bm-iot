package in.dream_lab.bm.stream_iot.tasks.filter;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This task should only be run from a single thread to avoid overwriting output filter file.
 *  
 * @author shukla, simmhan
 *
 */
public class BloomFilterTrain extends AbstractTask {

	private static final Object SETUP_LOCK = new Object();
	// static fields common to all threads
	private static int useMsgField;
	private static boolean doneSetup = false;
	
	private static int expectedInsertions;
	private static int insertionRange;
	private static double falsePositiveRatio;
	private static String bloomFilterFilepath;
	
	// local fields assigned to each thread
	private BloomFilter<String> bloomFilter;

	
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) { // Do setup only once for this task
				useMsgField = Integer.parseInt(p_.getProperty("FILTER.BLOOM_FILTER_CHECK.USE_MSG_FIELD", "0")); // If positive, use that particular field number in the input CSV message as input for count

				expectedInsertions = Integer.parseInt(p_.getProperty("FILTER.BLOOM_FILTER_TRAIN.EXPECTED_INSERTIONS", "20000000"));
				falsePositiveRatio = Double.parseDouble(p_.getProperty("FILTER.BLOOM_FILTER_TRAIN.FALSEPOSITIVE_RATIO", "0.1"));
				bloomFilterFilepath = (p_.getProperty("FILTER.BLOOM_FILTER.MODEL_PATH"));
				// autogerate random values with range that is half as the number of insertions 
				// so that each item has an insertion count of appox 2
				insertionRange = expectedInsertions/2;

				doneSetup=true;
				if(l.isInfoEnabled()) {
					if(useMsgField<0)
					l.info("CHECK: set message field  in properties file ,currently training with random  values");
					else
					l.info("CHECK: training done using column number {} as message field .",useMsgField);
				}
			}
		}
		Funnel<String> memberFunnel = new Funnel<String>() {
			public void funnel(String memberId, PrimitiveSink sink) {
				sink.putString(memberId, Charsets.UTF_8);
			}
		};
		bloomFilter = BloomFilter.create(memberFunnel, expectedInsertions, falsePositiveRatio);
	}


	/**
	 * Calling Bloom filter to train valid values
	 * https://google.github.io/guava/releases/snapshot/api/docs/com/google/common/hash/BloomFilter.html
	 */
	@Override
	protected Float doTaskLogic(String m) {
		if(useMsgField>0) {
			bloomFilter.put(m.split(",")[useMsgField-1]);
		}
		else {
			bloomFilter.put(String.valueOf(ThreadLocalRandom.current().nextInt(insertionRange)));
		}
		return Float.valueOf(1); // no output message
	}


	@Override
	public float tearDown(){		
		float result = super.tearDown();
		writeBloomFilterTofile(bloomFilter, bloomFilterFilepath, l);
		if(l.isInfoEnabled()) {
			l.info("Current False positive ratio is - {} ", bloomFilter.expectedFpp());
			l.info("written bloomFilter trained file to: " + bloomFilterFilepath);
		}
		return result;
	}

	/***
	 *
	 * @param bloomFilter
	 * @param bloomFilterFilepath
     * @param l
     */

	public static void writeBloomFilterTofile(BloomFilter<String> bloomFilter, String bloomFilterFilepath, Logger l) {
		// Write the BloomFilter into the file, for use later in checking
		try(FileOutputStream fos = new FileOutputStream(new File(bloomFilterFilepath))) {
			bloomFilter.writeTo(fos);
			fos.flush();
		} catch (IOException e) {			
			l.warn("Could not open file: " + bloomFilterFilepath, e);
		}
	}
}
