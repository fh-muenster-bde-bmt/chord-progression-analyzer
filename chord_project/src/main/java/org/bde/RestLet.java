package org.bde;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.Client;
import org.restlet.Context;
import org.restlet.data.Protocol;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestLet {

	private static String address = "http://10.60.67.3:9999/HBase/";
	final static Logger logger = LoggerFactory.getLogger(RestLet.class);

	/**
	 * Festlgen der Client-Konfiguration für die Nutzung der Rest-Schnittstelle
	 * 
	 * @return Client-Konfiguration
	 */
	private static Client initializeClient() {
		Client client = new Client(new Context(), Protocol.HTTP);
		client.getContext().getParameters().add("socketTimeout", "1000000000");
		client.getContext().getParameters().add("maxIdleTimeout", "1000000000");
		client.getContext().getParameters().add("idleTimeout", "1000000000");
		client.getContext().getParameters().add("connectionTimeout", "1000000000");
		client.getContext().getParameters().add("maxIoIdleTimeMs", "0");
		client.getContext().getParameters().add("ioMaxIdleTimeMs", "0");
		client.getContext().getParameters().add("readTimeout", "1000000000");
		return client;
	}

	/**
	 * Abrufen der Score Informationen für eine Künster-Titel-Kombination von
	 * einem Restful-Webservice
	 * 
	 * @param id
	 *            Übergabe Parameter an die Rest-Route -> id einer
	 *            Künster-Titel-Kombination (id stammt aus Solr)
	 * @return Ergbenisse der Rückgabe der Rest-Route in Form von Listen
	 */
	public static ArrayList<List<String>> getScore(String id) {

		ClientResource resource = new ClientResource(address + "getScore");
		resource.setNext(initializeClient());
		resource.setRetryOnError(false);

		Representation reply = resource.post(id);

		ArrayList<List<String>> returnValue = new ArrayList<List<String>>();

		try {

			JSONArray jsonArray = new JSONArray(reply.getText());

			for (int i = 0; i < jsonArray.length(); i++) {

				ArrayList<String> entry = new ArrayList<String>();
				JSONObject jsonObject = (JSONObject) jsonArray.get(i);

				entry.add(jsonObject.getString("artist"));
				entry.add(jsonObject.getString("title"));

				double score = Math.round((Double.valueOf(jsonObject.getString("score")) * 100) * 100.0) / 100.0;

				entry.add(score + " %");

				returnValue.add(entry);
			}

		} catch (JSONException e) {
			logger.info("Fehler beim Parsen des Replys in ein JSONArray. " + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			logger.info("Fehler beim Abholen des Replys von der Rest-Route. " + e.getMessage());
			e.printStackTrace();
		}

		return returnValue;
	}

}