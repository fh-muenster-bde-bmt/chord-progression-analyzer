package bde.FrontendHBaseAccess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bde.FrontendHBaseAccess.rest.RestfulWebservice;

public class App {
	final static Logger logger = LoggerFactory.getLogger(App.class);
	@SuppressWarnings("unused")
	private static RestfulWebservice rw;

	public static void main(String[] args) {

		try {
			rw = new RestfulWebservice();
		} catch (Exception e) {
			logger.info("Fehler beim starten des RestfulWebservice. " + e.getMessage());
			e.printStackTrace();
		}
		
		logger.info("------------ Ende ------------");
	}
}
