/*
 * This file is meant to be a fix for the error listed below
 *
 * ###################################
 * ERROR: Read past end of the file supplier.dat: page = 7021788454467754630 length = 1
 * ###################################
 * 
 * TODO: replace the main.cc contents with the contents of this file and it will clear the page count
 */

#include <iostream>
#include "Catalog.h"
#include "DBFile.h"
#include "Schema.h"
#include "Config.h"
#include "File.h"
#include "Record.h"

#include <string.h>
#include <vector>

using namespace std;

int main()
{
	string db = "catalog.sqlite";
	Catalog catalog(db);

	vector<string> tableNames;
	catalog.GetTables(tableNames);

	cout << "First for" << endl;
	vector<string> heapFilePaths, textFilePaths;
	for (int i = 0; i < tableNames.size(); i++)
	{
		heapFilePaths.push_back(tableNames[i] + ".dat");
		textFilePaths.push_back(tableNames[i] + ".tbl");
	}

	cout << "2 for" << endl;
	for (int i = 0; i < tableNames.size(); i++)
	{
		Schema schema;
		if (!catalog.GetSchema(tableNames[i], schema))
		{
			cout << "ERROR: Table does NOT exist!" << endl;
		}
		else
		{
			char *heapFilePath = new char[heapFilePaths[i].length() + 1];
			char *textFilePath = new char[textFilePaths[i].length() + 1];
			strcpy(heapFilePath, heapFilePaths[i].c_str());
			strcpy(textFilePath, textFilePaths[i].c_str());

			DBFile dbf;
			dbf.Create(heapFilePath, Heap);
			cout << "Create" << endl;
			dbf.Load(schema, textFilePath);
			cout << "Load" << endl;
			dbf.Close();
			cout << "Close" << endl;

			catalog.SetDataFile(tableNames[i], heapFilePaths[i]);
		}
	}
	cout << "Save" << endl;
	catalog.Save();
}
