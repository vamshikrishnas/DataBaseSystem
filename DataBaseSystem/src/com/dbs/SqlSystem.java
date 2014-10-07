
package com.dbs;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.ESqlStatementType;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TGroupByItemList;
import gudusoft.gsqlparser.nodes.TOrderByItemList;
import gudusoft.gsqlparser.nodes.TResultColumnList;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import gudusoft.gsqlparser.nodes.TTableList;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;

public class SqlSystem {

	DBSystem dbs;
	
	private HashMap<String,LinkedHashMap<String,String>> tableWithColumnNames = new HashMap<String,LinkedHashMap<String,String>>();
	private LinkedHashSet<String> printColumnSet = new LinkedHashSet<String>();
	private HashMap<String,Integer> groupBySet = new HashMap<String,Integer>();
	private HashMap<String,Integer> havingMap = new HashMap<String,Integer>(); 
	private String inputQueryString;
	private StringBuilder distinctAttribute = new StringBuilder();
	private boolean distinct = false;
	
	
	// Initializes the tableWithColumnNames <tableName, <ColumnName,ColumnType> >
	
	public SqlSystem() {
	
		dbs=new DBSystem();
		
		dbs.readConfig("/home/vamshi_s/Downloads/DBSystemWorkspace/test/");
		dbs.populateDBInfo();
		//initiaLizeTableName();
	}
	
	void initiaLizeTableName()
	{
		try
		{
			String dataPath;
			BufferedReader configFileReader = new BufferedReader(new FileReader(dbs.configFile));
			for(int i=0;i<2;i++)
				configFileReader.readLine();
			
			dataPath = configFileReader.readLine();
			dataPath = dataPath.substring(dataPath.indexOf(' ')+1, dataPath.length());
			String tempString = configFileReader.readLine();
			StringBuilder tableName = new StringBuilder();
			boolean tableStartFlag=true,tableNameFlag=true;
			
			while(tempString !=null )
			{
					if(tempString.equals("BEGIN"))
					{
						tempString=configFileReader.readLine();
						if(tempString==null)
								break;
						tableName.setLength(0);
						tableName.append(tempString);
						LinkedHashMap<String, String> columnData = new LinkedHashMap<String,String>();
						// After Reading Table Name, go wiht the columns
						while( (tempString=configFileReader.readLine()) !=null)
						{
							if(tempString.equals("END"))
							{
								tableWithColumnNames.put(tableName.toString(), columnData);
								break;
							}else
							{
								String []column = {"",""};
								column = tempString.split(",");
								column[0] = removeSpace(column[0]).toString();
								column[1] = removeSpace(column[1]).toString();
								columnData.put(column[0],column[1].toUpperCase());
							}
						}
					}
					tempString=configFileReader.readLine();
			}
		
		Iterator<String> it = tableWithColumnNames.keySet().iterator();
		String tempTableName = null;
		while(it.hasNext()){
			tempTableName = it.next();
			System.out.println("Table:"+tempTableName);
			Iterator<String> it1 = tableWithColumnNames.get(tempTableName).keySet().iterator();
			LinkedHashMap<String,String> h1= tableWithColumnNames.get(tempTableName); 
			while(it1.hasNext()){
				String col = it1.next();
				System.out.print(col+":"+h1.get(col)+",");
			}
			System.out.println("");
		}
		configFileReader.close();
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	StringBuilder removeSpace(String columnAttribute){
		
		int l = columnAttribute.length();
		char ch;
		StringBuilder numberString = new StringBuilder();
		numberString.setLength(0);
		for(int k=0;k<l;k++){
			ch = columnAttribute.charAt(k);
			if(ch!=' ')
				numberString.append(ch);
		}
		return numberString;
	}
	
	/*
	Determine the type of the query (select/create) and
	invoke appropriate method for it.
	*/
	
	public void queryType(String query) 
	{
		
		this.inputQueryString=query;
		int len=query.length();
		StringBuilder q=new StringBuilder(query.trim());
		int i=0;
		//trim all the empty spaces !
		while((i<len)&& (q.charAt(i)==' '))
		{	
			i++;
		}
		
		if(i<len)
		{
			if(q.substring(i, i+6).equalsIgnoreCase("select"))
				selectCommand(query);
			else if(q.substring(i, i+6).equalsIgnoreCase("create"))
				createCommand(query);
			else
			{
				System.out.println("Query Invalid !");
				System.exit(0);
			}
		}else
		{
			System.out.println("Inavlid Query !");
			System.exit(0);
		}
	
	}
	
	/*
	Use any SQL parser to parse the input query. Check if the table doesn't exists
	and execute the query.
	The execution of the query creates two files : <tablename>.data and
	<tablename>.csv. An entry should be made in the system config file used
	in previous deliverable.
	Print the query tokens as specified at the end.
	**format for the file is given below
	*/
	
	public void createCommand(String query)
	{
		
		//System.out.println("CREATE COMMAND");
		
		try
		{
			TGSqlParser parser = new TGSqlParser(EDbVendor.dbvoracle);
			parser.sqltext=query;
			int ret = parser.parse();
			if(ret==0)
			{
				int length = parser.sqlstatements.size();
				for(int i=0;i<length;i++)
				{
					analyzeStmt(parser.sqlstatements.get(i));
				}
				
				
			}else
			{
				System.out.println("Error in Parsing SQL statement : ERROR ");
				System.out.println(parser.getErrormessage());
				System.exit(0);
			}
			
			
			
		}catch(Exception e)
		{
			System.out.println("Exception in selectCommand Method");
			e.printStackTrace();
		}
	
		
		
		
	}

	/*
	Use any SQL parser to parse the input query. Perform all validations (table
	name, attributes, datatypes, operations). Print the query tokens as specified
	below.
	
	Ref : http://www.dpriver.com/blog/list-of-demos-illustrate-how-to-use-general-sql-parser/decoding-sql-grammar-select-statement/
	Ref : http://www.dpriver.com/blog/list-of-demos-illustrate-how-to-use-general-sql-parser/get-columns-in-select-list/
	Ref : http://www.dpriver.com/blog/list-of-demos-illustrate-how-to-use-general-sql-parser/get-columns-in-select-list/
	*/
	public void selectCommand(String query)
	{
		//System.out.println("SELECT COMMAND");
		
		try
		{
			TGSqlParser parser = new TGSqlParser(EDbVendor.dbvoracle);
			parser.sqltext=query;
			int ret = parser.parse();
			if(ret==0)
			{
				int length = parser.sqlstatements.size();
				for(int i=0;i<length;i++)
				{
					analyzeStmt(parser.sqlstatements.get(i));
				}
			}
			else
			{
				System.out.println("Error in Parsing SQL statement : ERROR ");
				System.out.println(parser.getErrormessage());
				System.exit(0);
			}
		}catch(Exception e)
		{
			System.out.println("Exception in selectCommand Method");
			e.printStackTrace();
		}
	
	
	}
	
	
	/*
	 * This Method analyzes the statement specified and calls the appropriate method.
	 * 
	 */
	
	 private void analyzeStmt(TCustomSqlStatement stmt){

	        switch(stmt.sqlstatementtype){
	            case sstselect:
	                analyzeSelectStmt((TSelectSqlStatement)stmt);
	                break;
	            case sstcreatetable:
	                analyzeCreateTableStmt((TCreateTableSqlStatement)stmt);
	                break;
	            default:
	                System.out.println(stmt.sqlstatementtype.toString());
	        }
	    }
	
	 /*
	  * 
	  * 1 . Check for existence of table ! 
	  * 2 . Creates two files <tablename>,data and <tablename>.csv
	  * 3.  Update config File
	  * 4.  Print the Query Tokens.
	  */
	 
	
	private void analyzeCreateTableStmt(TCreateTableSqlStatement stmt) {
		// TODO Auto-generated method stub
		
		String tablename = stmt.getTableName().toString(); 
		
		// check for existence of tableName
		if(dbs.checkTable(tablename))
		{
			System.out.println("Table Exits in the DataBase : Cant create !");
			//System.exit(0);
		}else
		{
			
			StringBuilder attributeType = new StringBuilder();
			Vector<String> attributeVector = new Vector<String>();
			int numberOfColumns = stmt.getColumnList().size();
			int indexOfSpace;
			
			for(int i=0;i<numberOfColumns;i++)
			{
				//System.out.println(stmt.getColumnList().getColumn(i).toString());
				attributeVector.add(stmt.getColumnList().getColumn(i).toString());
				indexOfSpace=attributeVector.get(i).indexOf(' ');
				attributeType.setLength(0);
				attributeType.append(attributeVector.get(i).substring(indexOfSpace+1,attributeVector.get(i).length()).toString());
				System.out.println(attributeType);
				if(!checkAttributetype(attributeType))
				{
					System.out.println("Invalid Attributes !");
					return ;
				}
			}
			
			StringBuilder pathtoTable=new StringBuilder(dbs.pathTables);
			StringBuilder configFilePath= new StringBuilder(dbs.configFile);
			pathtoTable.append(tablename);
			
			File tabledataFile=new File(pathtoTable+".data");
			File tablecsvFile=new File(pathtoTable+".csv");
			
			// Print the Info & write to Config file & write csv and data files. 
			
			if(!tabledataFile.exists() && !tablecsvFile.exists())
			{
				try
				{
					System.out.println("QueryType:create");
					System.out.println("TableName:"+tablename);
					System.out.print("Attributes:");
					int vectLength = attributeVector.size();
					
					// stores attribute and attribute type :
					
					LinkedHashMap<String,String> tempColumnMap = new LinkedHashMap<String,String>();
					String columnName = null,colAttributeType=null;
					StringBuilder attribute=new StringBuilder();
					attribute.setLength(0);
					for(int vectIndex=0;vectIndex<vectLength;vectIndex++){
						
						indexOfSpace = attributeVector.get(vectIndex).indexOf(' ');
						columnName = attributeVector.get(vectIndex).substring(0, indexOfSpace);
						colAttributeType = attributeVector.get(vectIndex).substring(indexOfSpace+1,attributeVector.get(vectIndex).length());
						attribute.append(columnName);
						attribute.append(":");
						attribute.append(colAttributeType);
						
						if(vectIndex<vectLength-1)
							attribute.append(",");
						/*Make new entry in table HashMap along with column Name*/
						tempColumnMap.put(columnName,colAttributeType.toUpperCase());
					}
					
					
					/*write the data into file*/			
					tabledataFile.createNewFile();
					tablecsvFile.createNewFile();
					
					BufferedWriter tableDataWriter = new BufferedWriter(new FileWriter(tabledataFile));
					tableDataWriter.write(attribute+"\n");
					tableDataWriter.close();
					
					BufferedWriter configWriter = new BufferedWriter(new FileWriter(configFilePath.toString(),true));
					configWriter.write("BEGIN\n"+tablename+"\n");
					//StringBuilder tempString = new StringBuilder();
			
					configWriter.write(attribute.toString().replace(",","\n").replace(":", ",")+"\n");
					configWriter.write("END\n");
					configWriter.close();
					
					System.out.println(attribute.toString().replace(":", " "));
					attributeVector.clear();
					
					tableWithColumnNames.put(tablename, tempColumnMap);
					// Update in the tablenames Vector 
					
					
					
				}catch(Exception e)
				{
					e.printStackTrace();
				}
			}else
			{
				System.out.println("Error in Writing");
			}
		}
	}

	private boolean checkAttributetype(StringBuilder attributeType) {
		// TODO Auto-generated method stub
		
		String attType = attributeType.toString().toUpperCase();
		
		if(attType.charAt(0)=='I' || attType.charAt(0)=='F')
		{
			if(attType.equalsIgnoreCase("INTEGER") || attType.equalsIgnoreCase("FLOAT"))
				return true;
			else 
				return false;
		}
		else{ 
			
			int lastIndex = attType.indexOf("VARCHAR(");
			if(lastIndex>=0){
				int index = attType.indexOf('(');
				for(int i = index+1;i < attType.length()-1;i++){
					if(attType.charAt(i)>='0' && attType.charAt(i)<='9');//do nothing continue
					else return false;
				}
				return true;
			}
			else return false;
		}
	}


	/*
	 * 1 . Validate Tables
	 * 2 . Validaate Columns
	 * 3 . Distict Columns
	 * 4 . Validate Where Condition
	 * 5 . Validate GroupBy
	 * 6 . Validate Order By
	 * 7 . Validate Having
	 * 
	 */
	
	private void analyzeSelectStmt(TSelectSqlStatement stmt) {
		// TODO Auto-generated method stubm
		
		//System.out.println("Analysing Select Statement");
		if(stmt.tables!=null)
		{
			/* Validate Tables 
			
			System.out.println(stmt.getResultColumnList().toString());
			System.out.println(stmt.getWhereClause().toString());
			System.out.println(stmt.getGroupByClause().toString());
			System.out.println(stmt.getOrderbyClause().toString());
			*/
			
			if(isTablesValid(stmt.tables)==false)
			{
				System.out.println("Table Doesnt Exist in the Database");
				System.out.println("Query Invalid");
				return;
			}
			
			if(!isColummnsValid(stmt.getResultColumnList(),stmt.tables))
			{
				System.out.println(" Column Doesnt Exist in Tables");
				System.out.println("Query Invalid");
				return;				
			}
			
			
			// Check for Distinct
			
			int distinctIndex = inputQueryString.toLowerCase().indexOf("distinct")+8;
			if(distinctIndex!=-1)
				distinct=true;
			
			if(stmt.getWhereClause()!=null && !validateWhereCondition(stmt.tables,stmt.getWhereClause().getCondition(),false))
			{
				System.out.println("Invalid Where Clause");
				System.out.println("Query Invalid");
				return;
			}
			
			if(stmt.getResultColumnList().size()==1 && stmt.getResultColumnList().getResultColumn(0).toString().equals("*") ){
				
				printAllColumns(stmt.tables,false);
				
			}
			else{
				printColumns(stmt.getResultColumnList(),false);
			}
			
			if(stmt.getOrderbyClause()!=null && !validateOrderBy(stmt.getOrderbyClause().getItems(),stmt.tables)){
				System.out.println("Failed in OrderBy");
				System.out.println("Query Invalid");
				return;
			}
			
			if(stmt.getGroupByClause()!=null)
			{
				if(!isValidateGroupBy(stmt.getGroupByClause().getItems()))
				{
					System.out.println("Group by clause failed");
					System.out.println("Query Invalid");
					return ;
				}
				if(stmt.getGroupByClause().getHavingClause()!=null){
					if(!isValidateHaving(stmt.getGroupByClause().getHavingClause(),stmt.tables)){
						System.out.println("Having failed");
						System.out.println("Query Invalid");
						return;
					}
				}
			}
			System.out.println("QueryType:select");
			System.out.println("TableName:"+ stmt.tables);
			System.out.print("Columns:");
			
			if(stmt.getResultColumnList().size()==1 && stmt.getResultColumnList().getResultColumn(0).toString().equals("*") ){
				
				printAllColumns(stmt.tables,true);
				
			}
			else{
				printColumns(stmt.getResultColumnList(),true);//+stmt.getResultColumnList());
			}
			System.out.print("Distict:");
			if(stmt.getSelectDistinct()!=null)
			{
				System.out.println(stmt.getSelectDistinct().toString());
			}
			else System.out.println("NA");
			
			System.out.print("Condition:");
			if(stmt.getWhereClause()!=null)
				System.out.println(stmt.getWhereClause().getCondition());
			else System.out.println("NA");
			
			System.out.print("Orderby:");
			if(stmt.getOrderbyClause()!=null){
				System.out.println(stmt.getOrderbyClause().getItems());
			}
			else System.out.println("NA");
			
			
			System.out.print("Groupby:");
			if(stmt.getGroupByClause()!=null){
				System.out.println(stmt.getGroupByClause().getItems());
			}
			else System.out.println("NA");
			
			System.out.print("Having:");
			if(stmt.getGroupByClause()!=null && stmt.getGroupByClause().getHavingClause()!=null){
				
				System.out.println(stmt.getGroupByClause().getHavingClause());
			}
			else 
				System.out.println("NA");
		}else
		{
			System.out.println("Select Statement : Invalid Query !");
		}
	}
	
	
	void printColumns(TResultColumnList columnList,boolean printFlag){
		
		int length = columnList.size();
		StringBuilder columnString = new StringBuilder();
		String str;
		groupBySet.clear();
		int cnt=0;
		for(int i=0;i<length;i++){
			columnString.setLength(0);
			columnString.append(columnList.getResultColumn(i).toString());
			str = removeParenthesis(columnString);
			columnString.setLength(0);
			columnString.append(str);
			str = checkColumn(str); /*add both tableName.colName & colName in groupBySet*/
			groupBySet.put(str,cnt++);groupBySet.put(columnString.toString(),cnt++);
			if(printFlag)
			System.out.print(columnString);
			if(i<(length-1) && printFlag)
				System.out.print(",");
		}
		if(printFlag)
		System.out.println("");
	}
	
	void printAllColumns(TTableList stmtTables,boolean printFlag){
		
		int length = stmtTables.size();
		String []tableNameArray = new String[length];
		
		tableNameArray = stmtTables.toString().split(",");
		String tempStr = null;
		LinkedHashSet<String> columnNames = new LinkedHashSet<String>();
		groupBySet.clear();
		
		int cnt=0;
		for(int j=0;j<length;j++){
			tempStr = tableNameArray[j];
			Iterator<String> it1 = tableWithColumnNames.get(tempStr).keySet().iterator();
			LinkedHashMap<String,String> h1= tableWithColumnNames.get(tempStr); 
			while(it1.hasNext()){
				columnNames.add(it1.next());
			}			
		}
		Iterator<String> columnIt = columnNames.iterator();
		String temp;
		while(columnIt.hasNext()){
			temp = columnIt.next();
			if(printFlag)
			System.out.print(temp);
			groupBySet.put(temp,cnt++);
			if(columnIt.hasNext() && printFlag)
				System.out.print(",");
		}
		if(printFlag)
		System.out.println("");
	
	}

	
	boolean validateOrderBy(TOrderByItemList orderByList,TTableList stmtTables)
	{
		TResultColumnList tempList = new TResultColumnList();
		if(!isColummnsValid(tempList, stmtTables))
		return false;
		return true;
	}
	
	boolean isValidateGroupBy(TGroupByItemList groupList){
		int length = groupList.size();
		int cnt=0;
		havingMap.clear();
		for(int i=0;i<length;i++){
			//System.out.println("**"+groupList.getGroupByItem(i)+"**");
			//System.out.println("**"+groupBySet.get(groupList.getGroupByItem(i).toString())+"**");
			havingMap.put(groupList.getGroupByItem(i).toString(),cnt++);
			if(groupBySet.get(groupList.getGroupByItem(i).toString())==null)
			//System.out.println("Failed ");
				return false;
		}
		return true;
	}
	
	boolean isValidateHaving(TExpression havingExpression,TTableList stmtTables){
		if(validateWhereCondition(stmtTables, havingExpression, true))
		return true;
		else return false;
	}
	

	private boolean validateWhereCondition(TTableList stmtTables,TExpression condition, boolean havingFlag) {
		// TODO Auto-generated method stub
		
		TResultColumnList conditionColumnList = new TResultColumnList();
		String [] columnAttribute = {"","",""};
		boolean intFlag=true,floatFlag=true;
		
		for(String str:condition.toString().split("AND|OR|and|or"))
		{
			intFlag=false;
			floatFlag=false;
			columnAttribute = str.split("=|<=|>=|<|>|<>");
			System.out.println(columnAttribute.length);
			
			if(columnAttribute.length>1)
			{
				
					if(columnAttribute[0].charAt(0)=='(')
						columnAttribute[0] = columnAttribute[0].substring(1);
					
					if(columnAttribute[1].charAt(columnAttribute[1].length()-1)==')')
						columnAttribute[1] = columnAttribute[1].substring(0, columnAttribute[1].length()-1);
					
					System.out.println("*********"+columnAttribute[0]);
					
					conditionColumnList.addResultColumn(columnAttribute[0].toString());
					System.out.println(conditionColumnList.size());
					try{
//						
						columnAttribute[0] = removeSpace(columnAttribute[0]).toString();
						columnAttribute[1] = removeSpace(columnAttribute[1]).toString();
						Integer.parseInt(columnAttribute[1]);
						intFlag = true;
						System.out.println(columnAttribute[0]+" "+columnAttribute[1]);
					}
					
					catch(NumberFormatException e){
						/*ignore the exception the string is not an integer*/
					}
					
					try{
						
						Float.valueOf(columnAttribute[1]);
						floatFlag = true;
					}
					catch(NumberFormatException e){
						
					}
					
					if(!intFlag && !floatFlag)
					{
						conditionColumnList.addResultColumn(columnAttribute[1]);
						columnAttribute[0] = checkColumn(columnAttribute[0]);
						columnAttribute[1] = checkColumn(columnAttribute[1]);
						if(havingFlag)
						{ 
							if(!isString(columnAttribute[1]) && (!havingMap.containsKey(columnAttribute[0]) || !havingMap.containsKey(columnAttribute[1]) ) )
							 return false;
							else if(isString(columnAttribute[1]) && getAttributeType(columnAttribute[0], stmtTables).indexOf("VARCHAR")!=0)
								return false;
						}
						else if(isString(columnAttribute[1])){
							if(getAttributeType(columnAttribute[0], stmtTables).indexOf("VARCHAR")!=0)/*VARCHAR not present*/
								return false;
						}
						else if(!checkColumnCompatible(columnAttribute[0],columnAttribute[1],stmtTables))
							return false;
						//System.out.println("column present");
					}
					else
					{/*either integer value or float value is present*/
						columnAttribute[0] = checkColumn(columnAttribute[0]);
						if( havingFlag && (!havingMap.containsKey(columnAttribute[0]) ) )
							 return false;
						if(intFlag && !floatFlag)
						{
							if (isColumnComparable(columnAttribute[0],stmtTables,"INTEGER") )
							{
							
							}
							else /*attribute is not compatible*/ 
								return false;
						}
						else if(intFlag && floatFlag){
							if (isColumnComparable(columnAttribute[0],stmtTables,"INTEGER") || isColumnComparable(columnAttribute[0],stmtTables,"FLOAT") ){
								//System.out.println("Integer & float");
							}
							else /*attribute is not compatible*/ 
								return false;
						}
						else{
							if(!isColumnComparable(columnAttribute[0],stmtTables,"FLOAT") || !isColumnComparable(columnAttribute[0], stmtTables,"INTEGER"))
								return false;
							//System.out.println("float only");
						}
					}
					
			}
			else{
				/*TODO String variable is present
				 * check for LIKE,add column to conditionColumnList
				 * */
				columnAttribute = str.split("LIKE|like");
				if(columnAttribute.length>1){
					
					columnAttribute[0] = removeSpace(columnAttribute[0]).toString();
					columnAttribute[1] = removeSpace(columnAttribute[1]).toString();
					columnAttribute[0] = checkColumn(columnAttribute[0]);
					if(havingFlag && havingMap.containsKey(columnAttribute[0]) )
						return false;
					if(isString(columnAttribute[1]) && getAttributeType(columnAttribute[0], stmtTables).indexOf("VARCHAR")!=0)/*VARCHAR not present*/
					return false;
				}
				else return false;
			}
		}
		//System.out.println("Where n");
		System.out.println(conditionColumnList.toString());
		
		//if(!isColummnsValid(conditionColumnList, stmtTables))
			//return false;
		return true;
		
	}

	String checkColumn(String column){
		int columnSplitLength ;
		String []tempColumn ={"",""};
		StringBuilder columnString = new StringBuilder();
		columnString.append(column);
		tempColumn = columnString.toString().split("\\.");
		columnSplitLength = tempColumn.length;//columnString.toString().split("\\.").length;
		if(columnSplitLength>1 && tableWithColumnNames.containsKey(tempColumn[0]) &&  tableWithColumnNames.get( tempColumn[0] ).containsKey( tempColumn[1] )){
			return tempColumn[1];//return column Name
		}
		return tempColumn[0];//return column Name
	}
	
	String getAttributeType(String column,TTableList stmtTables)
	{
		int tableNum = stmtTables.size();
		String tableName;
		for(int index=0;index<tableNum;index++){
			tableName= stmtTables.getTable( index).toString();
			if( tableWithColumnNames.containsKey( tableName ) ){
				if(tableWithColumnNames.get(tableName).containsKey(column))
				{
					System.out.println(tableWithColumnNames.get(tableName).get(column));
					return tableWithColumnNames.get(tableName).get(column);
				}
			}
		}
		return "NULL";
	}
	

	boolean isColumnComparable(String column,TTableList stmtTables,String attType){
		
		if(getAttributeType(column,stmtTables).equals(attType))
		return true;
		else return false;
	}

	boolean checkColumnCompatible(String column1,String column2,TTableList stmtTables){
		
		if(getAttributeType(column1, stmtTables).equals("NULL") || getAttributeType(column2, stmtTables).equals("NULL"))
			return false;
		
		if(getAttributeType(column1, stmtTables).equals(getAttributeType(column2, stmtTables)))
		return true;
		else return false;
	}

	
	
	private boolean isColummnsValid(TResultColumnList resultColumnList, TTableList tables) 
	{
		// TODO Auto-generated method stub
		
		int length = tables.size();
		HashSet<String> tableHashSet = new HashSet<String>();
		for(int i=0;i<length;i++)
		{
			tableHashSet.add(tables.getTable(i).toString());
		}
		
		int columnLenght = resultColumnList.size();
		String [] tableNameArray = new String[length];
		tableNameArray = tables.toString().split(",");
		
		StringBuilder columnString = new StringBuilder();
		int columnSplitLength;
		String []tempColumn = {"",""};
		
		/*HashSet for printing the columns*/
		printColumnSet.clear();
		
		System.out.println("HERE \n"+resultColumnList.toString());
		
		String str;
		
		for(int index=0;index<length;index++)
		{
			for(int i=0;i<columnLenght;i++)
			{
				if(columnLenght==1 && resultColumnList.getResultColumn(i).toString().equals("*"))
				{	
					//printAllColumns(.tables,false);
					return true;
				
				}	else
				{
					columnString.setLength(0);
					columnString.append(resultColumnList.getResultColumn(i).toString());
					str = removeParenthesis(columnString);
					
					columnString.setLength(0);
					columnString.append(str);
					
					tempColumn = columnString.toString().split("\\."); // e.id
					
					columnSplitLength = tempColumn.length; //columnString.toString().split("\\.").length;
					
					printColumnSet.add(columnString.toString());
					
					if( columnSplitLength==1 && tableWithColumnNames.get(tableNameArray[index]).containsKey( columnString.toString()) )
					{
						printColumnSet.add(columnString.toString());
					}
					else if(columnSplitLength>1 && tableHashSet.contains(tempColumn[0]) &&  tableWithColumnNames.get( tempColumn[0] ).containsKey( tempColumn[1] ))
					{
						//do nothing continue with next col
						printColumnSet.add(columnString.toString());
					}
				}
			}
		}
		
		for(int i=0;i<columnLenght;i++)
		{
			
				columnString.setLength(0);
				columnString.append(resultColumnList.getResultColumn(i).toString());
				str = removeParenthesis(columnString);
				columnString.setLength(0);
				columnString.append(str);
				if(!printColumnSet.contains(columnString.toString()))
					return false;
		}
			return true;
	}
		
		
	boolean isString(String str)
	{
		if( (str.charAt(0)=='\'' && str.charAt(str.length()-1)=='\'') || (str.charAt(0)=='\"' && str.charAt(str.length()-1)=='\"') )
		return true;
		return false;
	}
		
	
	private String removeParenthesis(StringBuilder columnString) {
		// TODO Auto-generated method stub
		String temp=null;
		if(columnString.charAt(0)=='(' && columnString.charAt(columnString.length()-1)==')'){
			temp = columnString.toString().substring(1,columnString.length()-1);
		}
		else
			temp = columnString.toString();
		return temp;
	}


	private boolean isTablesValid(TTableList tables) {
		// TODO Auto-generated method stub
		
		//System.out.println(tables.toString());
		int tables_size=tables.size();
		String []tablesInRelation = new String[tables_size];
		tablesInRelation=tables.toString().split(",");
		
		//Handle for alias
		String []tempTableName = {"","",""}; /*table_name AS Alias_name*/
		for(int i=0;i<tables_size;i++){
			tempTableName = tablesInRelation[i].split(" ");
			if( !tableWithColumnNames.containsKey(tempTableName[0]))
				return false;
		}
		
		return true;
	}

	

	public static void main(String[] args)
	{
		// The arguments must be given in double quotes !
		System.out.println(args[0]);
		String query=args[0];
		SqlSystem sqlsytem = new SqlSystem();
		sqlsytem.initiaLizeTableName();
		sqlsytem.queryType(query);
		
	}
	
}
