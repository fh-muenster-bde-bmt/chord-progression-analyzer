package bde.SparkPatternDetector;

public class Result {

	private Integer ngrams1Count;
	private Integer ngrams2Count;
	private Integer matches;
	private String artistA;
	private String keyA;
	private String titleA;
	private String artistB;
	private String keyB;
	private String titleB;

	public Result(Integer ngrams1Count, Integer ngrams2Count, Integer matches, String keyA, String artistA, String titleA, String keyB,
			String artistB, String titleB) {
		super();
		this.ngrams1Count = ngrams1Count;
		this.ngrams2Count = ngrams2Count;
		this.matches = matches;
		this.artistA = artistA;
		this.keyA = keyA;
		this.titleA = titleA;
		this.artistB = artistB;
		this.keyB = keyB;
		this.titleB = titleB;
	}

	public Integer getNgrams1() {
		return ngrams1Count;
	}

	public void setNgrams1(Integer ngrams1) {
		this.ngrams1Count = ngrams1;
	}

	public Integer getNgrams2() {
		return ngrams2Count;
	}

	public void setNgrams2(Integer ngrams2) {
		this.ngrams2Count = ngrams2;
	}

	public Integer getMatches() {
		return matches;
	}

	public void setMatches(Integer matches) {
		this.matches = matches;
	}

	public String getArtistA() {
		return artistA;
	}

	public void setArtistA(String artistA) {
		this.artistA = artistA;
	}

	public String getKeyA() {
		return keyA;
	}

	public void setKeyA(String keyA) {
		this.keyA = keyA;
	}

	public String getTitleA() {
		return titleA;
	}

	public void setTitleA(String titleA) {
		this.titleA = titleA;
	}

	public String getArtistB() {
		return artistB;
	}

	public void setArtistB(String artistB) {
		this.artistB = artistB;
	}

	public String getKeyB() {
		return keyB;
	}

	public void setKeyB(String keyB) {
		this.keyB = keyB;
	}

	public String getTitleB() {
		return titleB;
	}

	public void setTitleB(String titleB) {
		this.titleB = titleB;
	}

	public Double getDiceCoefficient() {
		return new Double((2 * matches.doubleValue()) / (ngrams1Count + ngrams2Count));
	}
}
