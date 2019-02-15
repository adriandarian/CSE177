#include <iostream>
#include <unordered_set>
#include "sqlite3.h"
#include <stdio.h>
#include <stdlib.h>
#include "Schema.h"
#include "Catalog.h"

using namespace std;


//TODO: I don't think this garbo is even needed
/*
struct AttsStruct {
	string name;
	string type;
	unsigned int noDistinct;
};
*/
	unordered_set <string> tablesHash;
	unordered_set <string> attsHash;



Catalog::Catalog(string& _fileName) {
	sqlite3 *db;
	char *zErrMsg = 0;
	int rc;

	rc = sqlite3_open("test.db", &db);

	if( rc ) {
		fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
	} else {
		fprintf(stderr, "Opened database successfully\n");
	}
	sqlite3_close(db);
}

Catalog::~Catalog() {
}

bool Catalog::Save() {
	//Update the database boi 
	//Option 1: Delete everything from db, insert everyting into db from catalog.
		//Do you always want to delete everything from the db?
	//Option 2: Find duplicates, update duplicates, delete those that exist only in catalog but not in db?
		//Insert the rest from catalog into DB
	return true;
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	TablesStruct tab;
	for(auto it = tablesList.begin(); it != tablesList.end(); it++) {
		tab = *it;
		if(tab.name == _table) {
			_noTuples = tab.noTuples;
			return true;
		}
	}
	return false;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
	//Loop through list of tables and delete
	TablesStruct tab;
	for(auto it = tablesList.begin(); it != tablesList.end(); it++) {
		tab = *it;
		if(tab.name == _table) {
			tab.noTuples = _noTuples;
			*it = tab;
		}
	}
	
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	TablesStruct tab;
	for(auto it = tablesList.begin(); it != tablesList.end(); it++) {
		tab = *it;
		if(tab.name == _table) {
			_path = tab.pathToFile;
			return true;
		}
	}
	return false;
}

void Catalog::SetDataFile(string& _table, string& _path) {
	TablesStruct tab;
	for(auto it = tablesList.begin(); it != tablesList.end(); it++) {
		tab = *it;
		if(tab.name == _table) {
			tab.pathToFile = _path;
			*it = tab;
		}
	}
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute, unsigned int& _noDistinct) {
	//TODO: Check mate. (esp return false)
	TablesStruct tab;
	for(auto it = tablesList.begin(); it != tablesList.end(); it++) {
		tab = *it;
		if(tab.name == _table) {
			for(int i = 0; i < tab.schema.GetAtts().size(); ++i) {
				if(tab.schema.GetAtts()[i].name == _attribute) {
					_noDistinct = tab.schema.GetDistincts(_attribute);
					return true;
				}
			}
		}
	}
	return false;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute, unsigned int& _noDistinct) {
	//TODO: Check mate
	TablesStruct tab;
//	vector<string> tempAttName;
//	vector<string> tempAttType;
//	vector<unsigned int> tempAttDistincts;
	for(auto it = tablesList.begin(); it != tablesList.end(); ++it) {
		tab = *it;
		if(tab.name == _table) {
			int attributeLocation = tab.schema.Index(_attribute);
			tab.schema.GetAtts()[attributeLocation].noDistinct = _noDistinct;
			//TODO: IDK about this
			for(int i = 0; i < tab.schema.GetAtts().size(); ++i) {
				if(tab.schema.GetAtts()[i].name == _attribute) {
					tab.schema.GetAtts()[attributeLocation].noDistinct = _noDistinct;
				}
			}
			//Schema newSchema(tempAttName,tempAttType, tempAttDistincts);
			//tab.schema.Swap(newSchema);

		}
	}
}

void Catalog::GetTables(vector<string>& _tables) {
	//TODO: Can we just push_back straight to tables?
	TablesStruct tab;
	for(auto it = tablesList.begin(); it != tablesList.end(); ++it) {
		tab = *it;
		_tables.push_back(tab.name);
	}
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	//TODO: Can we push_back straight to _attributes? do we need to check it at all?
	TablesStruct tab;
	vector<string> newAtts;
	for(auto it = tablesList.begin(); it != tablesList.end(); it++) {
		tab = *it;
		if(tab.name == _table) {
			for(int i = 0; i < tab.schema.GetAtts().size(); ++i) {
				_attributes.push_back(tab.schema.GetAtts()[i].name);
				//TODO: Is this needed instead?
				//newAtts.push_b3ack(tab.schema.GetAtts()[i].name);
				return true;
			}
			
		}
	}
	//_attributes = newAtts;
	return false;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	//Hashmap of schema and table?
	TablesStruct tab;
	for(auto it = tablesList.begin(); it != tablesList.end(); ++it) {
		tab = *it;
		if(tab.name == _table) {
			_schema = tab.schema;
		}
	}
	return true;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes, vector<string>& _attributeTypes) {
	// Check for a valid attribute type
	// Integer in main-2.cc INTEGER in main.cc
	for(int i = 0; i < _attributeTypes.size(); ++i) {
		//TODO: Prettify this
		if(_attributeTypes[i] != "Integer" || _attributeTypes[i] != "Float" || _attributeTypes[i] != "String")
			return false;
	}

	//Check for duplicate tables
	if(tablesHash.find(_table) != tablesHash.end()) { // Found duplicate
		return false;
	}
	else { //Not Found
		tablesHash.insert(_table);
	}

	//Check for duplicate attributes, Return false if duplicate is found
	for(int i = 0; i < _attributes.size(); ++i) {
		if(attsHash.find(_attributes[i]) != attsHash.end()) { //Found duplicate
			return false;
		}
		else { //Not Found
			//TODO: Add atts here instead
			attsHash.insert(_attributes[i]);
		}
	}
	//TODO: Ok this bullshit is way uglier than it needs to be. Simplify dum-dum
	//Add new table
	TablesStruct newTable;
	//TODO: Delete
	//AttsStruct newAtt;
	
	newTable.name = _table;
	//Do I even set these or just keep at null
	newTable.noTuples = 0;
	newTable.pathToFile="NoPath";
	//Have to list through total atts for the table
	for(int i = 0; i < _attributes.size(); ++i) {
		noDistinct.push_back(0); //For schema assigning
		/*
		newAtt.name = _attributes[i];
		newAtt.type = _attributeTypes[i];
		newAtt.noDistinct = 0;
		attsList.push_back(newAtt);
		*/
	}
	
    Schema newSchema(_attributes, _attributeTypes, noDistinct);

	newTable.schema = newSchema;
	tablesList.push_back(newTable);
	//Need a way to tie schema and table - Done. Added schema to TablesStruct - removed Atts
	//schemaList.push_back(Schema(_attributes, _attributeTypes, noDistinct));
	return true;
}

bool Catalog::DropTable(string& _table) {
	// Loop through list of tables and delete
	TablesStruct tab;
		//Check for duplicate tables
	if(tablesHash.find(_table) != tablesHash.end()) { // Found duplicate
		return false;
	}
	else { //Not Found
		tablesHash.erase(_table);
	}

	for(auto it = tablesList.begin(); it != tablesList.end();) {
		tab = *it;
		if(tab.name == _table) {
			it = tablesList.erase(it);
		}
		else {
			it++;
		}
	}
	return true;
}

/* Overload printing operator for Catalog.
	* Print the content of the catalog in a friendly format:
	* table_1 \tab noTuples \tab pathToFile
	* \tab attribute_1 \tab type \tab noDistinct
	* \tab attribute_2 \tab type \tab noDistinct
	* ...
	* table_2 \tab noTuples \tab pathToFile
	* \tab attribute_1 \tab type \tab noDistinct
	* \tab attribute_2 \tab type \tab noDistinct
	* ...
	* Tables/attributes are sorted in ascending alphabetical order.
	*/
ostream& operator<<(ostream& _os, Catalog& _c) {
	string typeConvert;
	// Loop through all tables. 
	for(int i = 0; i < _c.tablesList.size(); ++i) {
		//Print out tables with specified format
		printf("%s \t %u \t %s \n", _c.tablesList[i].name.c_str(), _c.tablesList[i].noTuples, _c.tablesList[i].pathToFile.c_str());
			//Loop through attributes for a tables
			for(int j = 0; j < _c.tablesList[i].schema.GetAtts().size(); ++i) {
				//string is needed. cannot get a type working in printf
				if(_c.tablesList[j].schema.GetAtts()[i].type == Integer) {
					typeConvert = "Integer";
				}
				else if(_c.tablesList[j].schema.GetAtts()[i].type == Float) {
					typeConvert = "Float";
				}
				else if(_c.tablesList[j].schema.GetAtts()[i].type == String) {
					typeConvert = "String";
				}
				//Print out attributes with specified format
				printf("%s \t %s \t %u \n", _c.tablesList[j].schema.GetAtts()[i].name.c_str(), typeConvert.c_str(), _c.tablesList[j].schema.GetAtts()[i].noDistinct);
			}
	}
	return _os;
}
