package bde.SparkDataProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	final static Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) {
		SparkManager sm = SparkManager.getInstance();

		if (sm.readJsonFromHDFS() == true) {
			if (sm.writeDataToHBase() == true) {
				sm.createSolrIndexFile();
			}
		}

		logger.info("------------ Ende ------------");
	}
}
