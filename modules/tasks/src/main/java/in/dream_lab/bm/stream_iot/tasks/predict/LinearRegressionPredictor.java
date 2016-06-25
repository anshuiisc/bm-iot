package in.dream_lab.bm.stream_iot.tasks.predict;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import org.slf4j.Logger;
import weka.classifiers.functions.LinearRegression;
import weka.core.Instance;
import weka.core.Instances;

import java.io.StringReader;
import java.util.Properties;

/**
 * This task is thread-safe, and can be run from multiple threads. 
 * 
 * @author shukla, simmhan
 *
 */
public class LinearRegressionPredictor extends AbstractTask<Float> {

	private static final Object SETUP_LOCK = new Object();
	// static fields common to all threads
	private static boolean doneSetup = false;
	private static int useMsgField;

	private static String modelFilePath;
	// Assuming arff for Sense-Your-City dataset
	private static final String SAMPLE_INPUT = "-0.1,65.3,367.38";
	// Encode the arff header for SYS as a constant string
//	private static final String SAMPLE_HEADER = "@RELATION sys_data\n" +
//			"\n" +
////			"@ATTRIBUTE Longi            NUMERIC\n" +
////			"@ATTRIBUTE Lat              NUMERIC\n" +
//			"@ATTRIBUTE Temp             NUMERIC\n" +
//			"@ATTRIBUTE Humid            NUMERIC\n" +
//			"@ATTRIBUTE Light            NUMERIC\n" +
//			"@ATTRIBUTE Dust             NUMERIC\n" +
//			"@ATTRIBUTE airquality           NUMERIC\n" +
//			"\n" +
//			"@DATA\n" +
//			"%header format";

	private static final String SAMPLE_HEADER = "@RELATION sys_data\n" +
			"\n" +
			"@ATTRIBUTE triptimeInSecs             NUMERIC\n" +
			"@ATTRIBUTE tripDistance            NUMERIC\n" +
			"@ATTRIBUTE fareAmount           NUMERIC\n" +
			"\n" +
			"@DATA\n" +
			"%header format";

	private static Instances instanceHeader;
	private static LinearRegression lr;

	/**
	 * @param l_
	 * @param p_
	 */
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if (!doneSetup) { // Do setup only once for this task
				// If positive use actual input for prediction else use
				// dummyInputConst
				useMsgField = Integer.parseInt(p_.getProperty("PREDICT.LINEAR_REGRESSION.USE_MSG_FIELD", "0"));

				modelFilePath = p_.getProperty("PREDICT.LINEAR_REGRESSION.MODEL_PATH");
				try {
					lr = (LinearRegression) weka.core.SerializationHelper.read(modelFilePath);
					if (l.isInfoEnabled())
						l.info("Model is {} ", lr.toString());

					instanceHeader = WekaUtil.loadDatasetInstances(new StringReader(SAMPLE_HEADER), l);
					if (l.isInfoEnabled())
						l.info("Header is {}", instanceHeader);
					assert instanceHeader != null;

					doneSetup = true;
				} catch (Exception e) {
					l.warn("error loading decision tree model from file: " + modelFilePath, e);
					doneSetup = false;
				}
			}
		}
	}

	@Override
	protected Float doTaskLogic(String m) {
		Instance testInstance = null;

		try {
			String[] testTuple;
			if (useMsgField > 0) {
				testTuple = m.split(",");
			} else {
				testTuple = SAMPLE_INPUT.split(",");
			}
			testInstance = WekaUtil.prepareInstance(instanceHeader, testTuple, l);
			int prediction = (int) lr.classifyInstance(testInstance);
			if (l.isInfoEnabled()) {
				l.info(" ----------------------------------------- ");
				l.info("Test data               : {}", testInstance);
				l.info("Test data prediction result {}", prediction);	
			}

			// set parent to have the actual predictions
			return super.setLastResult((float)prediction);

		} catch (Exception e) {
			l.warn("error with clasification of testInstance: " + testInstance, e);
			// set parent to have the actual predictions
			return super.setLastResult(Float.MIN_VALUE);
		}
	}
}
