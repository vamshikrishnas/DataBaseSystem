package com.dbs;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Scanner;

public class TablePageOffset 
{
	int startRecord;
	int endRecord;
	int offSet;
	public TablePageOffset() 
	{
		startRecord = 0;
		endRecord = 0;
		offSet = 0 ;
	}
	

}


/*



public void populateDBInfo()
{
	
	try
	{
		Iterator<String> it=tableNames.iterator();
		String table_name;
		String table_Fname;
		String record;
		
		int currentFree=0;
		int currentOffset,currentRecord,recordLength=0;
		int nextRecordOffset=0,nextRecordSize=0;
		int newLine = 0;
		
		boolean newPage=true,firstPage = true,pageAdded = false,lastRecord=false;
		
		TablePageOffset pageData = new TablePageOffset();
		
		// Reading all the tables Data into pages
		
		while(it.hasNext())
		{
			table_name=it.next();
			table_Fname=pathTables+table_name+".csv";
			
			Scanner tableReader = new Scanner(new File(table_Fname));
			tableReader.useDelimiter("\n");
			
			// just to get the length of file
			RandomAccessFile rf= new RandomAccessFile(new File(table_Fname),"r");
			fileLengthInfo.put(table_name,rf.length());
			rf.close();
			
			currentOffset = 0;
			currentRecord = 0;
			
			firstPage = true;
			newPage=true;
			
			nextRecordOffset=0;
			nextRecordSize=0;
			
			lastRecord = false ;
			
			
			// Read the rows of the table
			
			while(tableReader.hasNext())
			{
				if(newPage)
				{
					pageData = new TablePageOffset();
					pageData.startRecord=currentRecord;
					pageData.offSet=currentOffset;
					currentOffset+=nextRecordOffset;
					currentFree = pageSize-nextRecordSize;
					
					newPage= false;
					pageAdded=false;
					record=null;
					
				}else
				{
					record=tableReader.next();
					recordLength=record.length()+1;
					if(!tableReader.hasNext() && record!=null)
						lastRecord=true;
					currentOffset+=recordLength;
					/*
					if((currentFree-recordLength-1)<0 && (currentFree - recordLength)==0)
					{
						newLine = 0;						
					}
					
					currentFree-= (recordLength);
					currentRecord++;
					
					if(currentFree<0)
					{
						if(firstPage)
						{	
								currentRecord--;
								firstPage = false;
						}
						pageData.endRecord = currentRecord -1;
						currentOffset -= (recordLength); 
						
						nextRecordOffset = recordLength; 
						nextRecordSize = recordLength;	
						
						addRecordtoDB(table_name,pageData);
						
						newPage = true;
						pageAdded = true;
					}
				}
			}
			
			
			
			
			if(lastRecord && pageAdded)
			{
				pageData = new TablePageOffset(); 
				pageData.startRecord = currentRecord;
				pageData.offSet = currentOffset ;
				pageData.endRecord = currentRecord;
				addRecordtoDB(table_name,pageData);
				
			}else if(lastRecord && firstPage)
			{
				pageData.endRecord=currentRecord-1;
				addRecordtoDB(table_name, pageData);
			}
			else if(lastRecord && !pageAdded) 
			{
				pageData.endRecord = currentRecord;
				addRecordtoDB(table_name, pageData);
			}
			
			tableReader.close();
		}
		
		
	}catch(Exception e)
	{
		e.printStackTrace();
	}
	
	printDBinfo();
	
}
*/