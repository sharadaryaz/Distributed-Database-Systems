

import java.util.Comparator;
import java.util.HashMap;

public class ValueComparator implements Comparator<String>{
 
	HashMap<String, Double> map = new HashMap<String, Double>();
 
	public ValueComparator(HashMap<String, Double> map){
		this.map.putAll(map);
	}
 
	@Override
	public int compare(String a, String b) {

	    if (((Double) map.get(a)).doubleValue() < ((Double) map.get(b)).doubleValue()) {
	        return 1;
	    } else if ( ((Double) map.get(a)).doubleValue() == ((Double) map.get(b)).doubleValue()) {
	        return ((String)a).compareTo(((String)b));
	    } else {
	        return -1;
	    }
	}
	
	public static void main(String[] args) {
		double n = -123.456;
		
		System.out.println((int)n);
	}
	
}