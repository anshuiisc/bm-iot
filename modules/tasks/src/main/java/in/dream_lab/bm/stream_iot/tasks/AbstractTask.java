package in.dream_lab.bm.stream_iot.tasks;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

import java.util.Properties;

public abstract class AbstractTask<T> implements ITask<T> {

	protected Logger l;
	protected StopWatch sw;
	protected int counter;
	private T lastResult = null;
	
	@Override
	public void setup(Logger l_, Properties p_) {
		l = l_;
		sw = new StopWatch();
		sw.start();
		sw.suspend();
		counter = 0;
		l.debug("finished task setup");
	}
	
	
	/**
	 * Increments timer and counter for number of calls. Calls child class. 
	 * Returns a float value indicating the status or value of the execution.
	 * Float.MIN_FLOAT means there was an error. All other values are valid. 
	 * Null return means there is no output to pass on. 
	 */
	@Override
	public Float doTask(String m) {
		sw.resume();
		////////////////////////

		Float result = doTaskLogic(m);
		
		////////////////////////
		assert result >= 0;
		sw.suspend();
		counter++;		
		return result;
	}
	
	/**
	 * Returns the last result that was set by the child task in case there is a
	 * result other than the float returned by doTaskLogic() 
	 *  
	 * @return
	 */
	public T getLastResult() {
		return lastResult;
	}
	
	/**
	 * Set by child task if there are results other than the float 
	 * result that need to be accessed. 
	 * 
	 * @param r
	 */
	protected T setLastResult(T r) {
		lastResult = r;
		return lastResult;
	}
	
	/**
	 * To be implemented by child class
	 * 
	 * @param m
	 * @return
	 */
	protected abstract Float doTaskLogic(String m);

	
	@Override
	public float tearDown() {
		sw.stop();
		l.debug("finished task tearDown");
		return sw.getTime()/counter;
	}


}
