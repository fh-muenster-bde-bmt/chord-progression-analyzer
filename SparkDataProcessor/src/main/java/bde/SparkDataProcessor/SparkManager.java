package bde.SparkDataProcessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkManager {
	private static volatile SparkManager instance = null;

	private SparkConf sparkConf;
	private JavaSparkContext javaSparkcontext;
	private HiveContext hiveContext;
	private HBaseManager hbm = null;
	private final Logger logger = LoggerFactory.getLogger(SparkManager.class);

	private String tablename = "songData";
	private String tempTablename = "songData";
	private String jsonPath = "hdfs://sandbox.hortonworks.com:8020/user/hue/complete/out.json";
	private String solrHdfsPath = "/tmp/songData/";
	private String solrIndexFileName = "solr.csv";

	private SparkManager() {
		super();
		this.sparkConf = new SparkConf().setAppName("SparkDataProcessor");
		this.javaSparkcontext = new JavaSparkContext(sparkConf);
		this.hiveContext = new HiveContext(javaSparkcontext.sc());
		this.getHBaseManagerInstance();
	}

	/**
	 * Abruf der einzigen HBaseManager Instanz
	 */
	private void getHBaseManagerInstance() {
		if (this.hbm == null) {
			this.hbm = HBaseManager.getInstance();
		}
	}

	/**
	 * Initialisierung der einzigen Instanz dieser Klasse
	 *
	 * @return Single Instanz
	 */
	public static SparkManager getInstance() {
		if (instance == null) {
			synchronized (SparkManager.class) {
				if (instance == null) {
					instance = new SparkManager();
				}
			}
		}
		return instance;
	}

	/**
	 * Einlesen von Daten aus einer JSON-Datei
	 * 
	 * @return true, wenn das Einlesen der Daten erfolgreich war
	 */
	public boolean readJsonFromHDFS() {
		DataFrame df = this.hiveContext.read().json(this.jsonPath).toDF();
		df.registerTempTable(this.tempTablename);
		df.cache();
		// df.printSchema();
		logger.info("Count of JSON File as DataFrame: " + df.count());
		// df.show();

		List<Row> results = df.collectAsList();

		if (results.size() > 0) {
			if (!results.get(0).toString().equalsIgnoreCase("false")) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	/**
	 * Schreiben der in Spark eingelesenen Daten nach HBase
	 * 
	 * @return true, wenn das Schreiben der Daten nach HBase erfolgreich war
	 */
	public boolean writeDataToHBase() {

		List<String> columnFamilies = new ArrayList<String>();
		columnFamilies.add("artistSongMetaData");
		columnFamilies.add("acousticData");

		if (this.hbm == null) {
			getHBaseManagerInstance();
		}

		DataFrame df = this.hiveContext.table(this.tempTablename);
		List<Row> results = df.collectAsList();

		if (this.hbm.addTable(this.tablename, columnFamilies) == true) {

			if (results.size() > 0) {
				if (!results.get(0).toString().equalsIgnoreCase("false")) {
					for (Row row : results) {

						String rowKey = null;
						try {
							rowKey = HashGenerator.generateMD5(row.getString(row.fieldIndex("artist")), row.getString(row.fieldIndex("title")));
						} catch (HashGenerationException e) {
							logger.info("Fehler beim Erstellen des Hash-Wertes. " + e.getMessage());
							e.printStackTrace();
						}

						this.hbm.add(this.tablename, rowKey, "artistSongMetaData", "artist", row.getString(row.fieldIndex("artist")));
						this.hbm.add(this.tablename, rowKey, "artistSongMetaData", "title", row.getString(row.fieldIndex("title")));
						this.hbm.add(this.tablename, rowKey, "acousticData", "intervals", row.getList(row.fieldIndex("intervals")).toString());
						this.hbm.add(this.tablename, rowKey, "acousticData", "key", row.getString(row.fieldIndex("key")));
						this.hbm.add(this.tablename, rowKey, "acousticData", "mode", String.valueOf(row.getLong(row.fieldIndex("mode"))));
					}
				}
			} else {
				return false;
			}
		} else {
			return false;
		}

		return true;
	}

	/**
	 * Anlegen einer Solr-Index-Datei, auf Basis der in HBase gespeicherten
	 * Datens‰tze
	 */
	public void createSolrIndexFile() {
		ArrayList<ArrayList<String>> solrEntries = new ArrayList<ArrayList<String>>();
		ArrayList<String> solrEntry = null;

		// Key = Spaltenname & Value = Column Family
		Map<String, String> columns = new HashMap<String, String>();
		columns.put("artist", "artistSongMetaData");
		columns.put("title", "artistSongMetaData");
		List<List<Cell>> result = this.hbm.getAllRowsOfColumns(tablename, columns);

		for (List<Cell> listOfCells : result) {
			solrEntry = new ArrayList<String>();

			// rowKey der Zeile
			solrEntry.add(Bytes.toString(listOfCells.get(0).getRowArray(), listOfCells.get(0).getRowOffset(), listOfCells.get(0).getRowLength()));
			for (Cell cell : listOfCells) {
				// value der Zeile
				solrEntry.add(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}

			solrEntries.add(solrEntry);
		}

		SolrIndexGenerator.writeCsvFile(this.solrIndexFileName, this.solrHdfsPath, solrEntries);
	}

	/**
	 * Schlieﬂen des JavaSparkContext
	 * 
	 * @return true, wenn das Schlieﬂen des JavaSparkContext erfolgreich war
	 * @throws Exception
	 */
	public Boolean shutdown() throws Exception {
		if (javaSparkcontext != null) {
			javaSparkcontext.close();
			return true;
		}
		throw new Exception("JavaSparkContext in SparkManager is null");
	}

	@Override
	protected void finalize() throws Throwable {
		try {
			shutdown();
		} finally {
			super.finalize();
		}
		super.finalize();
	}

}
