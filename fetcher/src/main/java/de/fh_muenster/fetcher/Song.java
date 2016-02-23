package de.fh_muenster.fetcher;

import java.util.ArrayList;
import java.util.List;

public class Song {

	private String artist;
	private String title;
	private List<Integer> intervals;
	private String key;
	private Integer mode;
	
	public Song(String artist, String title) {
		super();
		this.artist = artist;
		this.title = title;
		this.intervals = new ArrayList<Integer>();
	}
	
	public String getArtist() {
		return artist;
	}
	public void setArtist(String artist) {
		this.artist = artist;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}

	public List<Integer> getIntervals() {
		return intervals;
	}

	public void setIntervals(List<Integer> intervals) {
		this.intervals = intervals;
	}
	
	public void addInterval(Integer interval) {
		this.intervals.add(interval);
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Integer getMode() {
		return mode;
	}

	public void setMode(Integer mode) {
		this.mode = mode;
	}
}
