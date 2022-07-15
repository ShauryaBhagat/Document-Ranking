package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.List;

/**
 * This is a simple utility to return the number of times a String term appears in the list of the strings provided.
 * 
 *
 */

public class TermCountCalculator {
	
	
	public short termCountCalc(String term, List<String> titleOrContent) {
		
		short termCount = 0;
		
		for (String s : titleOrContent) {
			
			if (term.equalsIgnoreCase(s)) termCount +=1;
			
		}		
		return termCount;
		
	}

}
