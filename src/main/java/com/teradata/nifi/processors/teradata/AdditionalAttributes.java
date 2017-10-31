package com.teradata.nifi.processors.teradata;

import org.apache.nifi.logging.ComponentLog;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Control additional attributes. Namely TABLE_ID and COMMIT_EPOCH.
 * 
 * @author juergenb
 *
 */
class AdditionalAttributes {
	private final static char[] ALPHABET = {
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
			'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
			'u', 'v', 'w', 'x', 'y', 'z'};

	private String tableId;
	private int commitEpoch;
	
	AdditionalAttributes(ComponentLog logger, int tableIdLength) {
		commitEpoch = 0;
		generateShortUUID(tableIdLength);

		logger.info("TABLE_ID = {}, COMMIT_EPOCH = {}", new Object[] {tableId, commitEpoch});
	}

	void incEpoch() {commitEpoch++;}

    String evaluate(String value) {
		if(value == null) return null;
		return value.replaceAll("%TABLE_ID%", tableId).replaceAll("%COMMIT_EPOCH%", Integer.toString(commitEpoch));
	}

	JSONObject getAsJson() {
		JSONObject json = new JSONObject();
		json.put("TABLE_ID", tableId);
		json.put("COMMIT_EPOCH", commitEpoch);
		return json;
	}

	Map<String, String> getAsMap() {
		Map<String, String> map = new HashMap<>();
		map.put("TABLE_ID", tableId);
		map.put("COMMIT_EPOCH", Integer.toString(commitEpoch));
		return Collections.unmodifiableMap(map);
	}

    private void generateShortUUID(int tableIdLength) {
		// Initialize random number generator uniquely for each node and thread.
		Random random = new Random();
		try {
			random.setSeed(InetAddress.getLocalHost().getHostAddress().hashCode() + Thread.currentThread().getId() + System.nanoTime());
		} catch (UnknownHostException e) {
			random.setSeed(Thread.currentThread().getId() + System.nanoTime());
		}

		// take randomly n digits from the alphabet -> (26+10)^10 possible table id's
		StringBuilder sb = new StringBuilder(tableIdLength);
		for(int i = 0; i < tableIdLength; i++) sb.append(ALPHABET[random.nextInt(ALPHABET.length)]);
		tableId = sb.toString();
	}
}
