package bde.FrontendHBaseAccess;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseManager {

	private static volatile HBaseManager instance = null;
	private Configuration hconf;
	private Admin admin;
	private Connection connection;

	final static Logger logger = LoggerFactory.getLogger(HBaseManager.class);

	private HBaseManager() {
		super();
		hconf = new Configuration();
		hconf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase-unsecure");
		try {
			connection = ConnectionFactory.createConnection(hconf);
			admin = connection.getAdmin();
		} catch (IOException e) {
			logger.info("Fehler beim Erstellen der Connection bzw. des Admins.");
			e.printStackTrace();
		}
	}

	/**
	 * Initialisierung der einzigen Instanz dieser Klasse
	 *
	 * @return Single Instanz
	 */
	public static HBaseManager getInstance() {
		if (instance == null) {
			synchronized (HBaseManager.class) {
				if (instance == null) {
					instance = new HBaseManager();
				}
			}
		}
		return instance;
	}

	/**
	 * Status des Clusters abrufen
	 * 
	 * @return Cluster Status
	 */
	public ClusterStatus getClusterStatus() {
		try {
			return admin.getClusterStatus();
		} catch (IOException e) {
			logger.info("Fehler beim Abrufen ClusterStatus.");
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * List alle Tabellen aus HBase aus
	 * 
	 * @return Liste der Tabellen als String
	 */
	public List<String> getTables() {
		logger.info("Lese Tabellen aus HBase aus.");
		TableName[] names = new TableName[0];
		try {
			names = admin.listTableNames();
		} catch (IOException e) {
			logger.info("Fehler beim Abrufen aller Tabellennamen.");
			e.printStackTrace();
		}
		List<String> tables = new ArrayList<String>();
		for (int i = 0; i < names.length; i++) {
			tables.add(names[i].getNameAsString());
		}
		return tables;
	}

	/**
	 * Auslesen einer Zeile zu einem Schlüssel
	 * 
	 * @param tablename
	 *            Quelltabelle
	 * @param rowKey
	 *            Schlüssel
	 * @return Liste aller Zellen in der Zeile
	 */
	public List<Cell> getRow(String tablename, String rowKey) {
		logger.info("Lese Zeile mit Schlüssel " + rowKey + " aus Tabelle " + tablename + ".");
		List<Cell> result = null;
		Result r = null;
		try {
			Table table = connection.getTable(TableName.valueOf(tablename));
			Get get = new Get(Bytes.toBytes(rowKey));
			r = table.get(get);
			result = r.listCells();
			table.close();
		} catch (IOException e) {
			logger.info("Fehler beim Auslesen einer Zeile.");
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Liest mehrere Zeilen ab einem bestimmten Zeilenschlüssel aus
	 * 
	 * @param tablename
	 *            Quelltabelle
	 * @param startKey
	 *            Ab welchem Schlüssel soll gelesen werden?
	 * @param lines
	 *            Wieviele Zeilen sollen maximal gelesen werden?
	 * @return Liste von Zeilen in Form einer Liste von Zellen
	 */
	public List<List<Cell>> getRows(String tablename, String startKey, int lines) {
		logger.info("Lese " + lines + " Zeilen aus Tabelle " + tablename + " ab Schlüssel " + startKey + " aus.");
		List<List<Cell>> rows = new ArrayList<List<Cell>>();

		try {
			Table table = connection.getTable(TableName.valueOf(tablename));
			Scan s = new Scan();
			s.setFilter(new PageFilter(lines));
			if (startKey != null) {
				s.setStartRow(Bytes.toBytes(startKey));
			}
			ResultScanner rs = table.getScanner(s);
			for (Result r2 = rs.next(); r2 != null; r2 = rs.next()) {
				rows.add(r2.listCells());
			}
			rs.close();
			table.close();
		} catch (IOException e) {
			logger.info("Fehler beim Auslesen mehrerer Zeilen.");
			e.printStackTrace();
		}

		return rows;
	}

	/**
	 * Liest alle Zeilen für die übergebenen Spalten einer Tabelle aus
	 * 
	 * @param tablename
	 *            Quelltabelle
	 * @param columns
	 *            Welche Spalten sollen ausgegben werden (Key = Spaltenname &
	 *            Value = Column Family)
	 * @return Liste von Zeilen in Form einer Liste von Zellen
	 */
	public List<List<Cell>> getAllRowsOfColumns(String tablename, Map<String, String> columns) {
		logger.info("Lese Zeilen aus Tabelle " + tablename + " aus.");

		List<List<Cell>> rows = new ArrayList<List<Cell>>();

		try {
			Table table = connection.getTable(TableName.valueOf(tablename));
			Scan scan = new Scan();
			scan.setCaching(100);

			for (Entry<String, String> entry : columns.entrySet()) {
				scan.addColumn(Bytes.toBytes(entry.getValue()), Bytes.toBytes(entry.getKey()));
			}

			ResultScanner rs = table.getScanner(scan);
			for (Result r2 = rs.next(); r2 != null; r2 = rs.next()) {
				rows.add(r2.listCells());

			}
			rs.close();
			table.close();
		} catch (IOException e) {
			logger.info("Fehler beim Auslesen mehrerer Zeilen.");
			e.printStackTrace();
		}

		return rows;
	}

	/**
	 * Deaktivieren einer Tabelle
	 * 
	 * @param tablename
	 *            Tabelle, die deaktiviert werden soll.
	 */
	public void disableTable(String tablename) {
		logger.info("Deaktiviere " + tablename + ".");
		try {
			admin.disableTable(TableName.valueOf(tablename));
		} catch (IOException e) {
			logger.info("Fehler beim Deaktivieren der Tabelle.");
			e.printStackTrace();
		}
	}

	/**
	 * Aktivieren einer Tabelle
	 * 
	 * @param tablename
	 *            Tabelle, die aktiviert werden soll
	 */
	public void enableTable(String tablename) {
		logger.info("Aktiviere Tabelle " + tablename + ".");
		try {
			admin.enableTable(TableName.valueOf(tablename));
		} catch (IOException e) {
			logger.info("Fehler beim Aktivieren der Tabelle.");
			e.printStackTrace();
		}
	}

	/**
	 * Löschen einer Tabelle (Muss zuvor deaktiviert worden sein)
	 * 
	 * @param tablename
	 *            Tabelle, die gelöscht werden soll
	 */
	public void deleteTable(String tablename) {
		logger.info("Lösche Tabelle " + tablename + ".");
		try {
			admin.deleteTables(tablename);
		} catch (IOException e) {
			logger.info("Fehler beim Löschen der Tabelle.");
			e.printStackTrace();
		}
	}

	/**
	 * Ist eine Tabelle aktiviert?
	 * 
	 * @param tablename
	 *            Tabelle, die überprüft werden soll
	 * @return true falls aktiv, false falls inaktiv
	 */
	public boolean isTableEnabled(String tablename) {
		logger.info("Überprüfe, ob Tabelle " + tablename + " aktiviert ist.");
		try {
			return admin.isTableEnabled(TableName.valueOf(tablename));
		} catch (IOException e) {
			logger.info("Fehler beim Abfragen des Tabellenstatus.");
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Löschen aller Zeilen in einer Tabelle. Tabelle wird dazu gelöscht und
	 * neuerstellt.
	 * 
	 * @param tablename
	 *            Tabelle, die geleert werden soll.
	 */
	public void emptyTable(String tablename) {
		logger.info("Leere Tabelle " + tablename + ".");
		HTableDescriptor td;
		try {
			td = admin.getTableDescriptor(TableName.valueOf(tablename));
			admin.disableTable(TableName.valueOf(tablename));
			admin.deleteTable(TableName.valueOf(tablename));
			admin.createTable(td);
		} catch (Exception e) {
			logger.info("Fehler beim Leeren der Tabelle.");
			e.printStackTrace();
		}
	}

	/**
	 * Auslesen aller Column-Families in einer Tabelle
	 * 
	 * @param tablename
	 *            Tabelle, die betrachtet werden soll
	 * @return Liste der Namen aller Column-Families als String
	 */
	public List<String> getColumnFamilies(String tablename) {
		logger.info("Lese ColumnFamilies für Tabelle " + tablename + " aus.");
		HTableDescriptor td;
		List<String> cfs = new ArrayList<String>();
		try {
			td = admin.getTableDescriptor(TableName.valueOf(tablename));
			HColumnDescriptor[] descr = td.getColumnFamilies();
			for (int i = 0; i < descr.length; i++) {
				cfs.add(descr[i].getNameAsString());
			}
		} catch (Exception e) {
			logger.info("Fehler beim Leeren der Tabelle.");
			e.printStackTrace();
		}
		return cfs;
	}

	/**
	 * Überprüfen, ob eine Tabelle existiert
	 * 
	 * @param tablename
	 *            Tabelle, deren Existenz überprüft werden soll.
	 * @return true, falls existent, false falls nicht
	 */
	public boolean tableExists(String tablename) {
		logger.info("Überprüfe, ob Tabelle " + tablename + " existiert.");
		try {
			return admin.tableExists(TableName.valueOf(tablename));
		} catch (IOException e) {
			logger.info("Fehler beim Abfragen der Existenz einer Tabelle.");
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Löschen einer Zeile mit einem bestimmten Zeilenschlüssel
	 * 
	 * @param tablename
	 *            Tabelle, aus der gelöscht werden soll
	 * @param rowKey
	 *            Schlüssel der zu löschenden Zeile
	 */
	public void deleteRow(String tablename, String _rowKey) {
		logger.info("Lösche Zeile mit Schlüssel " + _rowKey + " aus Tabelle " + tablename + " aus.");
		try {
			Table table = connection.getTable(TableName.valueOf(tablename));
			Delete d = new Delete(Bytes.toBytes(_rowKey));
			table.delete(d);
			table.close();
		} catch (IOException e) {
			logger.info("Fehler beim Löschen des Datensatzes mit Schlüssel " + _rowKey + " aus Tabelle " + tablename + ".");
			e.printStackTrace();
		}
	}

	/**
	 * Suchen nach Zeilen mit einer bestimmten Kombination aus Column-Family,
	 * Spalte und Wert
	 * 
	 * @param tablename
	 *            Tabelle, in der gesucht werden soll
	 * @param columnFamily
	 *            Column-Family, in der gesucht werden soll
	 * @param column
	 *            Spalte, in der gesucht werden soll
	 * @param value
	 *            Wert, der in der Spalte vorhanden sein muss
	 * @return Liste aller Zeilen, die auf die Suchparameter zutreffen
	 */
	public List<List<Cell>> search(String tablename, String columnFamily, String column, String value) {
		logger.info("Führe komplexe Suche in " + tablename + " nach ColumnFamily " + columnFamily + ", Spalte " + column + " und Wert " + value
				+ " aus.");
		List<List<Cell>> rows = new ArrayList<List<Cell>>();

		try {
			Table table = connection.getTable(TableName.valueOf(tablename));

			Scan s = null;

			if ((columnFamily != null) && (column != null) && (value != null)) {
				s = new Scan();
				SingleColumnValueFilter f = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(column), CompareOp.EQUAL,
						Bytes.toBytes(value));
				f.setFilterIfMissing(true);
				s.setFilter(f);
				ResultScanner rs = table.getScanner(s);
				for (Result r2 = rs.next(); r2 != null; r2 = rs.next()) {
					rows.add(r2.listCells());
				}
				rs.close();
				table.close();
			}
		} catch (IOException e) {
			logger.info("Fehler beim Zugriff auf die Tabelle.");
			e.printStackTrace();
		}
		return rows;
	}

	/**
	 * Hinzufügen einer neuen Zeile oder einer neuen Spalte zu einer Tabelle
	 * 
	 * @param tablename
	 *            Zieltabelle
	 * @param rowKey
	 *            Neuer oder existierender Schlüssel
	 * @param columnFamily
	 *            Existierende Column-Family
	 * @param column
	 *            Neue oder existierende Spalte
	 * @param value
	 *            Neuer Wert
	 */
	public void add(String tablename, String rowKey, String columnFamily, String column, String value) {
		logger.info("Füge Spalte zu Tabelle " + tablename + " an Key " + rowKey + " hinzu (" + columnFamily + ":" + column + "=" + value + ")");
		try {
			Table table = connection.getTable(TableName.valueOf(tablename));
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
			table.put(put);
			table.close();
		} catch (IOException e) {
			logger.info("Fehler beim Zugriff auf die Tabelle.");
			e.printStackTrace();
		}
	}

	/**
	 * Anlegen einer neuen Tabelle inklusive Column Families
	 * 
	 * @param tablename
	 *            Name der Tabelle
	 * @param columnFamilies
	 *            Liste mit Namen der Column Families
	 */
	public boolean addTable(String tablename, List<String> columnFamilies) {

		if (tableExists(tablename) == false) {
			HTableDescriptor td = new HTableDescriptor(TableName.valueOf(tablename));

			for (String family : columnFamilies) {
				td.addFamily(new HColumnDescriptor(family));
			}
			try {
				admin.createTable(td);
			} catch (IOException e) {
				logger.info("Fehler beim erstellen der Tabelle.");
				e.printStackTrace();
				return false;
			}
		} else {
			logger.info("Tabelle existiert bereits.");
		}
		return true;
	}

	@Override
	protected void finalize() throws Throwable {
		try {
			connection.close();
		} finally {
			super.finalize();
		}
		super.finalize();
	}
}
