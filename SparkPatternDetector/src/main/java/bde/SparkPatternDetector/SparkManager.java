package bde.SparkPatternDetector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.NGram;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkManager {
	private static volatile SparkManager instance = null;

	private SparkConf sparkConf;
	private JavaSparkContext javaSparkcontext;
	private HiveContext hiveContext;
	private HBaseManager hbm = null;
	private final Logger logger = LoggerFactory.getLogger(SparkManager.class);

	private String tablenameRead = "songData";
	private String tablenameWrite = "scoreData";
	private String tempTablename = "songData";
	private int nGramN = 3;

	private SparkManager() {
		super();
		this.sparkConf = new SparkConf().setAppName("SparkPatternDetector");
		this.javaSparkcontext = new JavaSparkContext(sparkConf);
		this.hiveContext = new HiveContext(javaSparkcontext.sc());
		getHBaseManagerInstance();
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
	 * Lesen der Daten aus HBase
	 * 
	 * @return true, wenn das Lesen der Daten aus HBaseerfolgreich war
	 */
	public boolean readDataFromHBase() {
		JSONArray jArray = null;

		// Key = Spaltenname & Value = Column Family
		Map<String, String> columns = new HashMap<String, String>();
		columns.put("artist", "artistSongMetaData");
		columns.put("title", "artistSongMetaData");
		columns.put("intervals", "acousticData");

		List<List<Cell>> rowlist = hbm.getAllRowsOfColumns(this.tablenameRead, columns);
		List<Row> sparkRowList = new ArrayList<Row>();

		for (List<Cell> hbaseRow : rowlist) {

			String chords = Bytes.toString(hbaseRow.get(0).getValueArray(), hbaseRow.get(0).getValueOffset(), hbaseRow.get(0).getValueLength());

			List<String> chordList = new ArrayList<String>();
			try {
				jArray = new JSONArray(chords);
				for (int i = 0; i < jArray.length(); i++) {
					chordList.add(jArray.getString(i));
				}
			} catch (JSONException e) {
				logger.info("Fehler beim Parsen der in HBase gespeicherten Intervalle in ein JSONArray. " + e.getMessage());
				e.printStackTrace();
				return false;
			}

			Row sparkRow = RowFactory.create(
					Bytes.toString(hbaseRow.get(0).getRowArray(), hbaseRow.get(0).getRowOffset(), hbaseRow.get(0).getRowLength()),
					Bytes.toString(hbaseRow.get(1).getValueArray(), hbaseRow.get(1).getValueOffset(), hbaseRow.get(1).getValueLength()),
					Bytes.toString(hbaseRow.get(2).getValueArray(), hbaseRow.get(2).getValueOffset(), hbaseRow.get(2).getValueLength()), chordList);

			sparkRowList.add(sparkRow);
		}

		StructType schema = new StructType(new StructField[] { new StructField("key", DataTypes.StringType, false, Metadata.empty()),
				new StructField("artist", DataTypes.StringType, false, Metadata.empty()),
				new StructField("title", DataTypes.StringType, false, Metadata.empty()),
				new StructField("intervals", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty()) });

		DataFrame dfSongs = this.hiveContext.createDataFrame(sparkRowList, schema);
		dfSongs.registerTempTable(this.tempTablename);
		dfSongs.cache();

		// dfSongs.printSchema();
		// dfSongs.show();

		return true;
	}

	/**
	 * Erzeugen der NGrams
	 */
	public void calcNgrams() {

		DataFrame dfSongs = this.hiveContext.table(this.tempTablename);

		NGram ngramTransformer = new NGram().setInputCol("intervals").setOutputCol("ngrams").setN(nGramN);

		DataFrame dfNGram = ngramTransformer.transform(dfSongs);

		// dfNGram.printSchema();
		// dfNGram.show();

		Map<String, Result> matches = new HashMap<String, Result>();

		List<Row> results = dfNGram.select("ngrams", "key", "artist", "title").collectAsList();

		logger.info("results.size(): " + results.size());

		for (int i = 0; i < results.size(); i++) {
			List<String> ngrams_song1 = results.get(i).getList(0);

			for (int j = i + 1; j < results.size(); j++) {

				List<String> ngrams_song2 = results.get(j).getList(0);

				@SuppressWarnings("unchecked")
				List<String> intersection = (List<String>) CollectionUtils.intersection(ngrams_song1, ngrams_song2);

				matches.put((i + "-" + j),
						new Result(ngrams_song1.size(), ngrams_song2.size(), intersection.size(), results.get(i).getString(1),
								results.get(i).getString(2), results.get(i).getString(3), results.get(j).getString(1), results.get(j).getString(2),
								results.get(j).getString(3)));
			}
		}

		logger.info("matches.size(): " + matches.size());

		writeDataToHBase(matches);
	}

	/**
	 * Schreiben der Ergebnisse der Score-Berechnungen nach HBase
	 * 
	 * @return true, wenn das Schreiben der Daten nach HBase erfolgreich war
	 */
	private boolean writeDataToHBase(Map<String, Result> matches) {

		List<String> columnFamilies = new ArrayList<String>();
		columnFamilies.add("metaArtistA");
		columnFamilies.add("metaArtistB");
		columnFamilies.add("score");

		if (this.hbm == null) {
			getHBaseManagerInstance();
		}

		if (this.hbm.addTable(this.tablenameWrite, columnFamilies) == true) {

			for (Entry<String, Result> entry : matches.entrySet()) {

				Result result = entry.getValue();
				String rowKeyA = null;
				String rowKeyB = null;

				try {
					rowKeyA = HashGenerator.generateMD5(result.getKeyA(), result.getKeyB());
				} catch (HashGenerationException e) {
					logger.info("Fehler beim Erstellen des Hash-Wertes. " + e.getMessage());
					e.printStackTrace();
				}

				try {
					rowKeyB = HashGenerator.generateMD5(result.getKeyB(), result.getKeyA());
				} catch (HashGenerationException e) {
					logger.info("Fehler beim Erstellen des Hash-Wertes. " + e.getMessage());
					e.printStackTrace();
				}

				this.hbm.add(this.tablenameWrite, rowKeyA, "metaArtistA", "artist", result.getArtistA());
				this.hbm.add(this.tablenameWrite, rowKeyA, "metaArtistA", "title", result.getTitleA());
				this.hbm.add(this.tablenameWrite, rowKeyA, "metaArtistA", "key", result.getKeyA());
				this.hbm.add(this.tablenameWrite, rowKeyA, "metaArtistB", "artist", result.getArtistB());
				this.hbm.add(this.tablenameWrite, rowKeyA, "metaArtistB", "title", result.getTitleB());
				this.hbm.add(this.tablenameWrite, rowKeyA, "metaArtistB", "key", result.getKeyB());
				this.hbm.add(this.tablenameWrite, rowKeyA, "score", "score", result.getDiceCoefficient().toString());

				this.hbm.add(this.tablenameWrite, rowKeyB, "metaArtistA", "artist", result.getArtistB());
				this.hbm.add(this.tablenameWrite, rowKeyB, "metaArtistA", "title", result.getTitleB());
				this.hbm.add(this.tablenameWrite, rowKeyB, "metaArtistA", "key", result.getKeyB());
				this.hbm.add(this.tablenameWrite, rowKeyB, "metaArtistB", "artist", result.getArtistA());
				this.hbm.add(this.tablenameWrite, rowKeyB, "metaArtistB", "title", result.getTitleA());
				this.hbm.add(this.tablenameWrite, rowKeyB, "metaArtistB", "key", result.getKeyA());
				this.hbm.add(this.tablenameWrite, rowKeyB, "score", "score", result.getDiceCoefficient().toString());
			}
		}
		return true;
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
