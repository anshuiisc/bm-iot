package in.dream_lab.bm.stream_iot.tasks;

import org.slf4j.Logger;

import java.util.Properties;

public interface ITask<T> {
	public void setup(Logger l_, Properties p_);
	/**
	 * Task logic that will be executed on each input event seen.
	 * 
	 * @param m the input message
	 * @return null if there is no message output, Float.MIN_FLOAT if there is an error, output from the logic otherwise. 
	 */
	public Float doTask(String m);
	
	/**
	 * Returns the last result that was set by the child task in case there is a
	 * result other than the float returned by doTaskLogic() 
	 *  
	 * @return
	 */
	public T getLastResult();
	
	public float tearDown();	
}
