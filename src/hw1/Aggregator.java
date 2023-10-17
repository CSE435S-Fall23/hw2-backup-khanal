package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	
	 private AggregateOperator operator;
	    private boolean groupBy;
	    private TupleDesc td;
	    private HashMap<Field, Integer> countMap; 
	    private HashMap<Field, Integer> sumMap;
	    private HashMap<Field, Integer> maxMap;
	    private HashMap<Field, Integer> minMap;

	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		 this.operator = o;
        this.groupBy = groupBy;
        this.td = td;
        this.countMap = new HashMap<>();
        this.sumMap = new HashMap<>();
        this.maxMap = new HashMap<>();
        this.minMap = new HashMap<>();

	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		//your code here
		Field groupField = groupBy ? t.getField(0) : null;
		IntField valueField = (IntField) t.getField(1);  // Assuming the value is in the second field

		countMap.put(groupField, countMap.getOrDefault(groupField, 0) + 1);

		if (valueField != null) {
		    if (operator == AggregateOperator.SUM || operator == AggregateOperator.AVG) {
		        // SUM and AVG
		        int currentSum = sumMap.getOrDefault(groupField, 0);
		        sumMap.put(groupField, currentSum + valueField.getValue());
		    }

		    if (operator == AggregateOperator.MAX) {
		        // MAX
		        int currentValue = valueField.getValue();
		        int currentMax = maxMap.getOrDefault(groupField, Integer.MIN_VALUE);
		        maxMap.put(groupField, Math.max(currentMax, currentValue));
		    }

		    if (operator == AggregateOperator.MIN) {
		        // MIN
		        int currentValue = valueField.getValue();
		        int currentMin = minMap.getOrDefault(groupField, Integer.MAX_VALUE);
		        minMap.put(groupField, Math.min(currentMin, currentValue));
		    }
		} 


	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		 ArrayList<Tuple> results = new ArrayList<>();

	        for (Field group : countMap.keySet()) {
	            Tuple t = new Tuple(td);
	            int countValue = countMap.get(group);

	            switch (operator) {
	                case COUNT:
	                    t.setField(1, new IntField(countValue));
	                    break;
	                case SUM:
	                    int sumValue = sumMap.getOrDefault(group, 0);
	                    t.setField(1, new IntField(sumValue));
	                    break;
	                case AVG:
	                    int sum = sumMap.getOrDefault(group, 0);
	                    int count = countValue;
	                    int avgValue = (count != 0) ? sum / count : 0;
	                    t.setField(1, new IntField(avgValue));
	                    break;
	                case MAX:
	                    int maxValue = maxMap.getOrDefault(group, 0);
	                    t.setField(1, new IntField(maxValue));
	                    break;
	                case MIN:
	                    int minValue = minMap.getOrDefault(group, 0);
	                    t.setField(1, new IntField(minValue));
	                    break;
	            }

	            if (groupBy) {
	                t.setField(0, group);
	            }
	            results.add(t);
	        }

	        return results;
	}

}
