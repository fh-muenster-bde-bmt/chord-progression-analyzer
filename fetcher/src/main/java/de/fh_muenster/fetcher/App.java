package de.fh_muenster.fetcher;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;


/**
 * Hello world!
 *
 */
public class App 
{
	private final static String HDFS_NAMENODE = "hdfs://sandbox.hortonworks.com:8020";	
	private final static String PROXY_IP = "10.60.17.102";
	
    public static void main( String[] args )
    {   	
        // Künstler und Titel aus XML-Parsen
        ChartsXmlParser xmlparser = new ChartsXmlParser();
        List<Song> songs = xmlparser.getSongs();

        // Aufruf an Track-API und Track-ID ermitteln
		try {
			for(final Song song : songs){
				final List<String> trackIds = getTrackIds(song);
				fetchChordInformation(trackIds, song);
				System.out.println("Lade " + song.getArtist() + ", " + song.getTitle() + "...");
		    }
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        // JSON-Datei aus Ergebnissen zusammenbauen und in HDFS abspeichern
		String container_sucessful = "";
		String container_failed = "";
		for(final Song song : songs){
			JSONObject obj = new JSONObject();
			
			obj.put("artist", song.getArtist());
		    obj.put("title", song.getTitle());
		    obj.put("mode", song.getMode());
		    obj.put("key", song.getKey());
		    
		    JSONArray array = new JSONArray();
		    for(Integer interval : song.getIntervals()){
		    	array.put(interval.toString());
		    }
		    
		    obj.put("intervals", array);
			
		    if(song.getIntervals().size() > 0){
			    container_sucessful += obj.toString() + "\n";
			}
			else{
				container_failed += obj.toString() + "\n";
			}
		}
		
		try{
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", HDFS_NAMENODE);
            FileSystem fs = FileSystem.get(conf);
            BufferedWriter bw_sucessful_items =new BufferedWriter(new OutputStreamWriter(fs.create(new Path("/user/hue/complete/out.json"),true)));
            InputStream is_sucessful_items = new ByteArrayInputStream(container_sucessful.getBytes());
        	BufferedReader br_sucessful_items = new BufferedReader(new InputStreamReader(is_sucessful_items));
        	String line;
        	while ((line = br_sucessful_items.readLine()) != null) {
        		bw_sucessful_items.write(line);
        		bw_sucessful_items.newLine();
        	}
        	br_sucessful_items.close();
            bw_sucessful_items.close();
           
            BufferedWriter bw_failed_items =new BufferedWriter(new OutputStreamWriter(fs.create(new Path("/user/hue/failed/out.json"),true)));
            InputStream is_failed_items = new ByteArrayInputStream(container_failed.getBytes());
         	BufferedReader br_failed_items = new BufferedReader(new InputStreamReader(is_failed_items));
         	while ((line = br_failed_items.readLine()) != null) {
         		bw_failed_items.write(line);
         		bw_failed_items.newLine();
         	}
         	br_failed_items.close();
            bw_failed_items.close();
       } catch(Exception e){
    	   	System.err.println(e.getMessage());
       }
    }
    
    private static List<String> getTrackIds(final Song song) throws IOException{
    	boolean retry = false;
		int retryCount = 0;
		JSONTokener tokener = null;
		JSONObject root = null;
		URL url = new URL("http://api.musicgraph.com/api/v2/track/search?api_key=c8303e90962e3a5ebd5a1f260a69b138&title=" + URLEncoder.encode(song.getTitle().substring(0, song.getTitle().lastIndexOf(" (") == -1 ? song.getTitle().length() : song.getTitle().lastIndexOf(" (")), "UTF-8") + "&artist_name=" + URLEncoder.encode(song.getArtist(), "UTF-8"));
		do {
			Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(PROXY_IP, 8080));
			HttpURLConnection connection = (HttpURLConnection) url.openConnection(proxy);
			final int responseCode = connection.getResponseCode();
			if(responseCode != 200){
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				retry = true;
			  }
			else{
				tokener = new JSONTokener(connection.getInputStream());
				root = new JSONObject(tokener);
				retry = false;
			} 
			connection.disconnect();
			retryCount++;
		} while(retry && retryCount < 21);

		final List<String> trackIds = new ArrayList<String>();
		if(root != null){
			JSONArray array = (JSONArray) root.get("data");
			if(array != null && array.length() > 0){
				for(int i=0; i < array.length();i++){
					trackIds.add((String) array.getJSONObject(i).get("id"));
				}
			}
		}
    	return trackIds;
    }
    
    private static void fetchChordInformation(
    		final List<String> trackIds,
    		final Song song) throws IOException{
    	if(trackIds.size() > 0){
			int trackIndex = 0;
			JSONObject data = new JSONObject();
			boolean invalid = false;
			do{
				boolean retry = false;
				int retryCount = 0;
				JSONTokener tokener = null;
				JSONObject root = null;
				URL url = new URL("http://api.musicgraph.com/api/v2/track/" + trackIds.get(trackIndex) + "/acoustical-features?api_key=c8303e90962e3a5ebd5a1f260a69b138");
				do {
					Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(PROXY_IP, 8080));
					HttpURLConnection connection = (HttpURLConnection) url.openConnection(proxy);
					final int responseCode = connection.getResponseCode();
					if(responseCode != 200){
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						retry = true;
					  }
					else{
						tokener = new JSONTokener(connection.getInputStream());
						root = new JSONObject(tokener);
						retry = false;
					} 
					connection.disconnect();
					retryCount++;
				} while(retry && retryCount < 21);
				
				if(root != null){
					data = (JSONObject) root.get("data");
					if(data != null && data.length() > 0){
						JSONArray intervals = (JSONArray) data.get("intervals");
						for(int i=0; i < intervals.length(); i++){
							try{
								song.addInterval(Integer.parseInt((String)intervals.get(i)));
							} catch(final NumberFormatException e){
								invalid = true;
							}
						}
						try{
							song.setMode(Integer.parseInt((String)data.get("mode")));
						} catch(final NumberFormatException e){
							invalid = true;
						};
						song.setKey((String)data.get("key"));
						invalid = false;
					}
				}
				trackIndex++;
			} while((data.length() == 0 && trackIndex < trackIds.size()) || (invalid && trackIndex < trackIds.size()));
    	}
    }
}
