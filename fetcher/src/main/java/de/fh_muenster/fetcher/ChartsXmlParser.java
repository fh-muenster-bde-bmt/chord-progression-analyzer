package de.fh_muenster.fetcher;

import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public final class ChartsXmlParser {
	
	private final static String HDFS_NAMENODE = "hdfs://sandbox.hortonworks.com:8020";	
	private List<Song> tracks = new ArrayList<Song>();
	
	public ChartsXmlParser(){		
		 try {
		    	Configuration conf = new Configuration();
				conf.set("fs.defaultFS", HDFS_NAMENODE);
				FileSystem fs = FileSystem.get(conf);
			    FSDataInputStream in = fs.open(new Path("/user/hue/incoming/charts.xml"));
	
	        	DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
	        	DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	        	Document doc = dBuilder.parse(in);
	        	fs.close();
	        	doc.getDocumentElement().normalize();
	
	        	NodeList nodes = doc.getElementsByTagName("track");
	
	        	for (int i = 0; i < nodes.getLength(); i++) {
	
	        		Node node = nodes.item(i);
	
	        		if (node.getNodeType() == Node.ELEMENT_NODE) {
	
	        			Element element = (Element) node;
	
	        			String title = getValue("name", element);
	
	        			NodeList artist = element.getElementsByTagName("artist");
	        			String artist_name = getValue("name", (Element)artist.item(0));
	        			
	        			tracks.add(new Song(artist_name, title));
	        		}
	
	        	}
	
	        } catch (Exception ex) {
	        	ex.printStackTrace();
	        }
	}

	public List<Song> getSongs(){
        return tracks;
    }
    
   	private static String getValue(String tag, Element element) {

    	Node node = getNode(tag, element);

    	return node.getNodeValue();

    }
   	
   	private static Node getNode(String tag, Element element) {

    	NodeList nodes = element.getElementsByTagName(tag).item(0).getChildNodes();

    	Node node = (Node) nodes.item(0);

    	return node;
   }
}
