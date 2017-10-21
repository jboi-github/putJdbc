/**
 * 
 */
package com.teradata.nifi.processors.teradata;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;

/**
 * Control additional attributes. Namely TABLE_ID and COMMIT_EPOCH.
 * 
 * @author juergenb
 *
 */
public class AdditionalAttributes {
	private ComponentLog logger;
	private Random random;
	private String tableId;
	private int commitEpoch, tableIdLength;
	
	public AdditionalAttributes(ComponentLog logger, int tableIdLength) {
		this.logger = logger;
		this.tableIdLength = tableIdLength;
		
		random = getRandomizer();
		commitEpoch = 0;
		generateShortUUID();
       	logger.info("set TABLE_ID to " + tableId + ", COMMIT_EPOCH = " + commitEpoch);
	}
    
    public String evaluate(PropertyValue propertyValue) {return evaluate(propertyValue, null);}
    
    public String evaluate(PropertyValue propertyValue, FlowFile flowFile) {
    		String property = (flowFile == null)? 
    				propertyValue.getValue():
    				propertyValue.evaluateAttributeExpressions(flowFile).getValue();
    		
    		property = property.replaceAll("%TABLE_ID%", tableId).replaceAll("%COMMIT_EPOCH%", Integer.toString(commitEpoch));
    	    	
    		logger.info("re-evaluated: " + property + ", from: " + propertyValue.getValue());
    		return property;
    }

    // Set a random seed that is different on each node and task
    private Random getRandomizer() {
		Random random = new Random();
		
		try {
			random.setSeed(InetAddress.getLocalHost().getHostAddress().hashCode() + Thread.currentThread().getId() + System.nanoTime());
		} catch (UnknownHostException e) {
			random.setSeed(System.nanoTime());
		}
		return random;
    }
    
	private final static char[] ALPHABET = {
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 
			'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 
			'u', 'v', 'w', 'x', 'y', 'z'};

    public void generateShortUUID() {
    		StringBuilder sb = new StringBuilder(tableIdLength);
   		for(int i = 0; i < tableIdLength; i++) sb.append(ALPHABET[random.nextInt(ALPHABET.length)]);
   		tableId = sb.toString();
   		
       	logger.info("set TABLE_ID to " + tableId + ", COMMIT_EPOCH = " + commitEpoch);
	}

    public void incEpoch() {commitEpoch++;}
    
    public long addJitter(long value, double jitter) {
    		long range = (long) (value * jitter * (2.0 * random.nextDouble() - 1.0) + 0.5);
    		return value + range;
    }

	public String getTableId() {return tableId;}
	public int getCommitEpoch() {return commitEpoch;}
}
