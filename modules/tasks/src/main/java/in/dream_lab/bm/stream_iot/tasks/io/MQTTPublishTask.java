package in.dream_lab.bm.stream_iot.tasks.io;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This task is thread-safe, and can be run from multiple threads.
 *
 * @author shukla, simmhan
 *
 */
public class MQTTPublishTask extends AbstractTask implements MqttCallback {

	// static fields common to all threads
	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private static int useMsgField;

	private static String apolloUserName;
	private static String apolloPassword;
	private static String apolloURLlist;

	private static String topic;


	// local fields assigned to each thread
	private MqttClient mqttClient;
	private String apolloClient;
	private String   apolloURL;
	Random rn =new Random();

	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) { // ONLY for static fields
			if (!doneSetup) { // Do setup only once for this task
				// If positive, use that particular field number in the input CSV message as input for count
				useMsgField = Integer.parseInt(p_.getProperty("IO.MQTT_PUBLISH.USE_MSG_FIELD"));

				apolloUserName = p_.getProperty("IO.MQTT_PUBLISH.APOLLO_USER");
				apolloPassword = p_.getProperty("IO.MQTT_PUBLISH.APOLLO_PASSWORD");
//				apolloURL = p_.getProperty("IO.MQTT_PUBLISH.APOLLO_URL");
				//apolloClient = p_.getProperty("IO.MQTT_PUBLISH.APOLLO_CLIENT");

				topic = p_.getProperty("IO.MQTT_PUBLISH.TOPIC_NAME");
				apolloURLlist = p_.getProperty("IO.MQTT_PUBLISH.APOLLO_URLS");

//				try {
////					if(InetAddress.getLocalHost().getHostName().compareTo("anshustormscsup2d1")==0)
//				} catch (UnknownHostException e) {
//					e.printStackTrace();
//				}

				doneSetup = true;
			}
		}

		// setup for NON-static fields
		// generating random UUID client ID, getting connection to mqtt
		apolloClient = UUID.randomUUID().toString();
		int bid=rn.nextInt(apolloURLlist.split(",").length);
//		int bid=rn.nextInt(2);
		apolloURL=apolloURLlist.split(",")[bid];
		l.warn("Unqiue Broker-ID:"+bid+"-apolloURL-"+apolloURL);
		mqttClient = connectToMQTT(apolloURL, apolloClient, this, l);
		assert mqttClient != null;
		if (l.isInfoEnabled()) l.info("Client ID {} is connected.", mqttClient.getClientId());
	}

	@Override
	protected Float doTaskLogic(String m) {
		String input;
		if (useMsgField > 0) {
			input = m.split(",")[useMsgField - 1];
//			if (l.isInfoEnabled()) l.info("1-msg to publish is {} ", input);
		} else {
			input = String.valueOf(ThreadLocalRandom.current().nextInt(100));
//			if (l.isInfoEnabled()) l.info("2-msg to publish is {} ", input);
		}

		try { // publish the message
			if(!mqttClient.isConnected()) {
				l.warn("Client ID {} was not connected. Reconnecting...", mqttClient.getClientId());
				mqttClient = connectToMQTT(apolloURL,apolloClient, this, l); // connect on demand
			}


			mqttClient.publish(topic, input.getBytes(), 0, false);
		} catch (MqttException e) {
			l.warn("Exception when publishing mqtt message " + input +
					", to topic " + topic + ", using client " + mqttClient, e);
		}

		// set parent to have the actual predictions
		super.setLastResult(input);

		return Float.valueOf(input.length());
	}

	@Override
	public float tearDown() {
		float result = super.tearDown();
		try {
			// disconnecting the connection
			mqttClient.disconnect();
		} catch (MqttException e) {
			l.warn("Exception when closing mqtt client" + mqttClient, e);
		}
		if (l.isInfoEnabled()) {
			l.info("Connection closed !!");
		}
		return result;
	}

	public static MqttClient connectToMQTT(String apolloURL, String apolloClient, MqttCallback callback, Logger l) {

		try {
			// clean session, keep alive for 18hrs
			MqttConnectOptions connOpt = new MqttConnectOptions();
			connOpt.setCleanSession(true);
			connOpt.setKeepAliveInterval(64800);// 18 hr keep connection if no message will get from the client

			connOpt.setUserName(apolloUserName);
			connOpt.setPassword(apolloPassword.toCharArray());

			if (l.isInfoEnabled())
				l.info("Apollo Client {}, URL {}, Username {}, Pass {}", apolloClient, apolloURL, apolloUserName, apolloPassword);

			// client with no persistence
			MqttClient myClient = new MqttClient(apolloURL, apolloClient, null);
//			MqttAsyncClient myClient=new MqttAsyncClient(apolloURL,apolloClient,null);

			myClient.setCallback(callback);
			myClient.connect(connOpt);



			if (l.isInfoEnabled())
				l.info("Connected to Apollo thru client {} - ", myClient);

			return myClient;
		} catch (Exception e) {
			l.warn("unable to create apollo client ID " + apolloClient + " for URL " + apolloURL, e);
			return null;
		}
	}

	@Override
	public void connectionLost(Throwable e) {
		l.warn("MQTT connectionLost for client ID " + apolloClient, e);
	}

	@Override
	public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
	}
}