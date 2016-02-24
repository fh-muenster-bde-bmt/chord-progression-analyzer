package bde.SparkDataProcessor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrIndexGenerator {

	// Zeilen-Trennzeichen, welches in der CSV-Datei genutzt werden soll
	private static final String NEW_LINE_SEPARATOR = "\n";

	// Header der CSV-Datei
	private static final Object[] FILE_HEADER = { "id", "artist", "title" };

	private final static Logger logger = LoggerFactory.getLogger(SolrIndexGenerator.class);

	private SolrIndexGenerator() {
	}

	/**
	 * Erstellung einer Solr-Index-Datei in Form einer CSV-Datei & Hinzufügen
	 * einzelner Datensätze zur Datei (pro Zeile ein Datensatz)
	 * 
	 * @param fileName
	 *            Name der Datei inkl. Dateiendung
	 * @param hdfsPath
	 *            relativer HDFS-Pfad
	 * @param solrEntries
	 *            Datensätze, welche in die CSV-Datei geschrieben werden sollen
	 */
	public static void writeCsvFile(String fileName, String hdfsPath, ArrayList<ArrayList<String>> solrEntries) {

		BufferedWriter fileWriter = null;

		CSVPrinter csvFilePrinter = null;

		// Anlegen des CSVFormat Objektes mit "\n" als Zeilen-Trennzeichen
		CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator(NEW_LINE_SEPARATOR);

		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://sandbox.hortonworks.com:8020");

			FileSystem fs = FileSystem.get(conf);
			fileWriter = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(hdfsPath + fileName))));
			csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);

			// Header in die CSV-Datei schreiben
			csvFilePrinter.printRecord(FILE_HEADER);

			// Die einzelnen Einträge in die CSV-Datei schreiben
			for (ArrayList<String> entry : solrEntries) {
				csvFilePrinter.printRecord(entry);
			}

			logger.info("CSV file was created successfully !!!");

		} catch (Exception e) {
			logger.info("Error in CsvFileWriter !!!");
			e.printStackTrace();
		} finally {
			try {
				fileWriter.flush();
				fileWriter.close();
				csvFilePrinter.close();
			} catch (IOException e) {
				logger.info("Error while flushing/closing fileWriter/csvPrinter !!!");
				e.printStackTrace();
			}
		}
	}

}
