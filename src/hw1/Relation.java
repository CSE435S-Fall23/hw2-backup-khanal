package hw1;

import java.util.ArrayList;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		this.td = td;
		this.tuples = l;
				
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		ArrayList<Tuple> selectedTuples = new ArrayList<>();

        for (Tuple tuple : this.tuples) {
            Field tupleField = tuple.getField(field);
            if (tupleField.equals(operand)) {
                selectedTuples.add(tuple);
            }
        }

        // Assuming there's a constructor in Relation that accepts a TupleDesc and a list of tuples.
        return new Relation(selectedTuples, td);
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		   // Check if sizes of the fields and names lists match
	    if (fields.size() != names.size()) {
	        throw new IllegalArgumentException("Number of fields and names must be the same.");
	    }
	    
	    // Get current field names from TupleDesc
	    String[] currentFieldNames = new String[td.numFields()];
	    Type[] currentType = new Type[td.numFields()];
	    for (int i = 0; i < td.numFields(); i++) {
	        currentFieldNames[i] = td.getFieldName(i);
	        currentType[i] = td.getType(i);
	    }

	    // Update the specified fields with new names
	    for (int i = 0; i < fields.size(); i++) {
	        int fieldIndex = fields.get(i);
	        if (fieldIndex >= 0 && fieldIndex < currentFieldNames.length) {
	            currentFieldNames[fieldIndex] = names.get(i);
	        } else {
	            throw new IllegalArgumentException("Invalid field index: " + fieldIndex);
	        }
	    }

	    // Create a new TupleDesc with updated field names while keeping the types the same
	    TupleDesc updatedTupleDesc = new TupleDesc(currentType, currentFieldNames);
	    
	    // Update the TupleDesc for all the tuples in this relation
	    for (Tuple tuple : this.tuples) {
	        tuple.setDesc(updatedTupleDesc);
	    }
	    
	    // Assuming the relation class has a method to set its TupleDesc, we update it.
	    this.td = updatedTupleDesc; 

	    return this;
	}

	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
		ArrayList<Tuple> projectedTuples = new ArrayList<>();
	    Type[] newTypes = new Type[fields.size()];
	    String[] newFields = new String[fields.size()];

	    for (int i = 0; i < fields.size(); i++) {
	        int fieldIndex = fields.get(i);
	        newTypes[i] = td.getType(fieldIndex);
	        newFields[i] = td.getFieldName(fieldIndex);
	    }
	    TupleDesc newTd = new TupleDesc(newTypes, newFields);

	    for (Tuple t : tuples) {
	        Field[] newTupleFields = new Field[fields.size()];
	        for (int i = 0; i < fields.size(); i++) {
	            newTupleFields[i] = t.getField(fields.get(i));
	        }
	        Tuple newTuple = new Tuple(newTd);
	        for (int i = 0; i < newTupleFields.length; i++) {
	            newTuple.setField(i, newTupleFields[i]);
	        }
	        projectedTuples.add(newTuple);

	    }

	    return new Relation(projectedTuples, newTd);
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		ArrayList<Tuple> joinedTuples = new ArrayList<>();

		// Construct a combined TupleDesc for the join result
		int numFieldsThis = this.td.numFields();
		int numFieldsOther = other.td.numFields();
		Type[] combinedTypes = new Type[numFieldsThis + numFieldsOther];
		String[] combinedFields = new String[numFieldsThis + numFieldsOther];

		// Populate types and fields for 'this' relation
		for (int i = 0; i < numFieldsThis; i++) {
		    combinedTypes[i] = this.td.getType(i);
		    combinedFields[i] = this.td.getFieldName(i);
		}

		// Populate types and fields for 'other' relation
		for (int i = 0; i < numFieldsOther; i++) {
		    combinedTypes[numFieldsThis + i] = other.td.getType(i);
		    combinedFields[numFieldsThis + i] = other.td.getFieldName(i);
		}

		TupleDesc combinedTupleDesc = new TupleDesc(combinedTypes, combinedFields);

		// Join using a nested loop
		for (Tuple t1 : this.tuples) {
		    for (Tuple t2 : other.tuples) {
		        if (t1.getField(field1).equals(t2.getField(field2))) {
		            Tuple newTuple = new Tuple(combinedTupleDesc);

		            // Set fields from 'this' relation
		            for (int i = 0; i < numFieldsThis; i++) {
		                newTuple.setField(i, t1.getField(i));
		            }

		            // Set fields from 'other' relation
		            for (int i = 0; i < numFieldsOther; i++) {
		                newTuple.setField(numFieldsThis + i, t2.getField(i));
		            }

		            joinedTuples.add(newTuple);
		        }
		    }
		}

		return new Relation(joinedTuples, combinedTupleDesc);

	}

	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		TupleDesc newTd;
	    
	    if (op.equals(AggregateOperator.COUNT)) {
	        Type[] types = groupBy ? new Type[]{ this.td.getType(0), Type.INT } : new Type[]{ Type.INT };
	        newTd = new TupleDesc(types, null);
	    } else {
	        newTd = this.td;
	    }
	    
	    Aggregator agg = new Aggregator(op, groupBy, newTd);
	    for (Tuple t : this.tuples) {
	        agg.merge(t);
	    }

	    return new Relation(agg.getResults(), newTd);
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return this.tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		String str = "";
	    str += td.toString() + ("\n");  
	    
	    for (Tuple tuple : tuples) {
	        str += tuple.toString();
	    }
	    
	    return str;
	}
}
