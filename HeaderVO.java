

public class HeaderVO {
	private String baseHeader;
	private String newheader;
	private double matchPercentage;

	
	public HeaderVO(String baseHeader, String newheader, double matchPercentage) {
		super();
		this.baseHeader = baseHeader;
		this.newheader = newheader;
		this.matchPercentage = matchPercentage;
	}

	public String getBaseHeader() {
		return baseHeader;
	}

	public void setBaseHeader(String baseHeader) {
		this.baseHeader = baseHeader;
	}

	public String getNewheader() {
		return newheader;
	}

	public void setNewheader(String newheader) {
		this.newheader = newheader;
	}

	public double getMatchPercentage() {
		return matchPercentage;
	}

	public void setMatchPercentage(double matchPercentage) {
		this.matchPercentage = matchPercentage;
	}

}
