package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		
		Catalog cat = Database.getCatalog();
		Table mainTable = (Table) sb.getFromItem();
		int tableIdentifier = cat.getTableId(mainTable.getFullyQualifiedName());
		ArrayList<Tuple> tuples = cat.getDbFile(tableIdentifier).getAllTuples();
		TupleDesc tupleDescription = cat.getDbFile(tableIdentifier).getTupleDesc();
		Relation rel = new Relation(tuples, tupleDescription);

		if (sb.getJoins() != null) { 
			rel = processJoins(cat, sb, rel);
		}

		if (sb.getSelectItems() != null) {
			rel = processSelect(sb, rel);
		}

		if (sb.getWhere() != null) {
			rel = processWhere(sb, rel);
		}

		return rel;
	}

	private Relation processWhere(PlainSelect sb, Relation rel) {
		WhereExpressionVisitor whereVisitor = new WhereExpressionVisitor();
		sb.getWhere().accept(whereVisitor);

		int fieldIdx = rel.getDesc().nameToId(whereVisitor.getLeft());
		return rel.select(fieldIdx, whereVisitor.getOp(), whereVisitor.getRight());
	}

	private Relation processJoins(Catalog cat, PlainSelect sb, Relation rel) {
		for (Join j : sb.getJoins()) {
			String tableName = j.getRightItem().toString();
			int tableId = cat.getTableId(tableName);
			ArrayList<Tuple> rightTableTuples = cat.getDbFile(tableId).getAllTuples();
			TupleDesc tupleDescRight = cat.getDbFile(tableId).getTupleDesc();
			Relation rightRel = new Relation(rightTableTuples, tupleDescRight);

			BinaryExpression joinCondition = (BinaryExpression) j.getOnExpression();
			int origFieldIdx = rel.getDesc().nameToId(((Column) joinCondition.getLeftExpression()).getColumnName());
			int rightFieldIdx = tupleDescRight.nameToId(((Column) joinCondition.getRightExpression()).getColumnName());
			rel = rel.join(rightRel, origFieldIdx, rightFieldIdx);
		}
		return rel;
	}

	private Relation processSelect(PlainSelect sb, Relation rel) {
		List<SelectItem> items = sb.getSelectItems();
		ArrayList<Integer> fieldsToSelect = new ArrayList<Integer>();
		boolean groupByPresent = sb.getGroupByColumnReferences() != null;

		if (!items.isEmpty() && !items.get(0).toString().equals("*")) {
			for (SelectItem item : items) {
				ColumnVisitor columnVisitor = new ColumnVisitor();
				item.accept(columnVisitor);

				if (columnVisitor.isAggregate()) {
					if (!groupByPresent) {
						ArrayList<Integer> aggregateFieldOnly = new ArrayList<>();
						aggregateFieldOnly.add(rel.getDesc().nameToId(columnVisitor.getColumn()));
						rel = rel.project(aggregateFieldOnly);
					}

					try {
						rel = rel.aggregate(columnVisitor.getOp(), groupByPresent);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				if (item instanceof SelectExpressionItem) {
					SelectExpressionItem expressionItem = (SelectExpressionItem) item;
					if (expressionItem.getAlias() != null) {
						String aliasName = expressionItem.getAlias().getName();
						ArrayList<String> newName = new ArrayList<String>();
						newName.add(aliasName);
						ArrayList<Integer> fieldNumber = new ArrayList<Integer>();
						fieldNumber.add(rel.getDesc().nameToId(columnVisitor.getColumn()));
						rel.rename(fieldNumber, newName);

						Column colToRename = (Column) expressionItem.getExpression();
						colToRename.setColumnName(aliasName);
						expressionItem.setExpression(colToRename);
						columnVisitor.visit(expressionItem);
					}

					fieldsToSelect.add(rel.getDesc().nameToId(columnVisitor.getColumn()));
				}
			}
			return rel.project(fieldsToSelect);
		}
		return rel;
		
	}
}
