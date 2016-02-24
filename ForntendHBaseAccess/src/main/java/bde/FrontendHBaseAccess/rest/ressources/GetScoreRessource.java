package bde.FrontendHBaseAccess.rest.ressources;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Post;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bde.FrontendHBaseAccess.HBaseManager;

public class GetScoreRessource extends ServerResource {

	private HBaseManager hbm = null;
	final static Logger logger = LoggerFactory.getLogger(GetScoreRessource.class);

	@Override
	protected void doInit() throws ResourceException {
		this.hbm = (HBaseManager) getContext().getAttributes().get("HBaseManager");
		super.doInit();
	}

	@Post("txt")
	public Representation getScore(String id) throws JSONException {

		logger.info("Value of id: " + id);

		if (id != null && !id.isEmpty()) {
			List<List<Cell>> result = hbm.search("scoreData", "metaArtistA", "key", id);

			Map<String, Double> res = new HashMap<String, Double>();
			for (List<Cell> listOfCells : result) {
				res.put(Bytes.toString(listOfCells.get(5).getRowArray(), listOfCells.get(5).getRowOffset(), listOfCells.get(5).getRowLength()),
						Double.valueOf(Bytes.toString(listOfCells.get(6).getValueArray(), listOfCells.get(6).getValueOffset(),
								listOfCells.get(6).getValueLength())));
			}

			Map<String, Double> resultSorted = sortByValue(res);
			JSONArray jArray = new JSONArray();
			int counter = 0;

			if (resultSorted.size() < 10) {
				counter = resultSorted.size();
			} else if (resultSorted.size() == 10) {
				return new JsonRepresentation("resultset is empty");
			} else {

				counter = 10;
			}

			for (Map.Entry<String, Double> entry : resultSorted.entrySet()) {
				if (counter > 0) {
					counter--;

					String key = entry.getKey();

					for (List<Cell> listOfCells : result) {

						if (Bytes.toString(listOfCells.get(0).getRowArray(), listOfCells.get(0).getRowOffset(), listOfCells.get(0).getRowLength())
								.equals(key)) {

							JSONObject jObject = new JSONObject();
							jObject.put("artist", Bytes.toString(listOfCells.get(3).getValueArray(), listOfCells.get(3).getValueOffset(),
									listOfCells.get(3).getValueLength()));
							jObject.put("title", Bytes.toString(listOfCells.get(5).getValueArray(), listOfCells.get(5).getValueOffset(),
									listOfCells.get(5).getValueLength()));
							jObject.put("id", Bytes.toString(listOfCells.get(4).getValueArray(), listOfCells.get(4).getValueOffset(),
									listOfCells.get(4).getValueLength()));
							jObject.put("score", Bytes.toString(listOfCells.get(6).getValueArray(), listOfCells.get(6).getValueOffset(),
									listOfCells.get(6).getValueLength()));

							jArray.put(jObject);

							break;
						}
					}
				} else {
					logger.info("Counter is 0");
					break;
				}
			}

			logger.info(jArray.toString());

			return new JsonRepresentation(jArray.toString());
		} else {
			return new JsonRepresentation("id is empty");
		}
	}

	public TreeMap<String, Double> sortByValue(Map<String, Double> res) {
		ValueComparator vc = new GetScoreRessource.ValueComparator(res);
		TreeMap<String, Double> sortedMap = new TreeMap<String, Double>(vc);
		sortedMap.putAll(res);
		return sortedMap;
	}

	class ValueComparator implements Comparator<String> {

		Map<String, Double> map;

		public ValueComparator(Map<String, Double> base) {
			this.map = base;
		}

		public int compare(String a, String b) {
			if (map.get(a) >= map.get(b)) {
				return -1;
			} else {
				return 1;
			} // returning 0 would merge keys
		}
	}
}
