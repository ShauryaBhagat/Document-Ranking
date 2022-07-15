package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

/**
 * Represents total term frequencies of the query terms 
 *
 */

public class TotalTF implements Serializable{
	
	private static final long serialVersionUID = 7309797023726062989L;
	
	int[] totalTermFrequencyInCorpus;
	
	public TotalTF () {}

	public TotalTF(int[] totalTermFrequencyInCorpus) {
		super();
		this.totalTermFrequencyInCorpus = totalTermFrequencyInCorpus;
	}

	public int[] getTotalTermFrequencyInCorpus() {
		return totalTermFrequencyInCorpus;
	}

	public void setTotalTermFrequencyInCorpus(int[] totalTermFrequencyInCorpus) {
		this.totalTermFrequencyInCorpus = totalTermFrequencyInCorpus;
	}
	
	

}
