#include <iostream>
#include <fstream>
#include <unordered_set>
#include <stdio.h>
#include <stdlib.h>
#include <string>

#include "sqlite3.h"
#include "Schema.h"
#include "Catalog.h"

using namespace std;

struct TablesStruct {
		string name;
		string pathToFile;
		Schema schema;
		unsigned int noTuples;
		//unsigned int tableCount;
	};

	vector<TablesStruct> tablesList;
	vector<unsigned int> noDistinct;



sqlite3 *db;
char *zErrMsg = 0;
int conn;
string sql;

static int callback(void *NotUsed, int argc, char **argv, char **azColName) {
	for (int i = 0; i < argc; i++) {
		printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
	}
	printf("\n");
	return 0;
}
string file;

Catalog::Catalog(string& _fileName) {
	// Setup a connection to the database
	conn = sqlite3_open(_fileName.c_str(), &db);

	file = _fileName;
	// check the database connection
	if (conn) 
		fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
	else 
		fprintf(stderr, "Opened database successfully\n");

	/* Create SQL statement */
	sql = "select name, pathToFile, noTuples from tables";

	/* Execute SQL statement */
	conn = sqlite3_exec(db, sql.c_str(), callback, 0, &zErrMsg);
   
	if (conn != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	} else {
		fprintf(stdout, "Table created successfully\n");
	}

	sqlite3_close(db);
}

Catalog::~Catalog() {
	Save();
}

bool Catalog::Save() {
	// Update the database boi 
	// Option 1: Delete everything from db, insert everyting into db from catalog.
		// Do you always want to delete everything from the db?
	// Option 2: Find duplicates, update duplicates, delete those that exist only in catalog but not in db?
		// Insert the rest from catalog into DB

	// Setup a connection to the database
	conn = sqlite3_open(file.c_str(), &db);
	
	TablesStruct tab;
	for(auto it = tablesList.begin(); it != tablesList.end(); it++) {
		tab = *it;
		/* Create SQL statement */
		string sql = "insert into tables (name, pathToFile, noTuples) " \
			"values (\'" + tab.name + "\', \'" + tab.pathToFile + "\', " + to_string(tab.noTuples) + ");";

		for (auto tableAttribute = 0; tableAttribute < tab.schema.GetAtts().size(); tableAttribute++) {
			auto tableAttributes = tab.schema.GetAtts()[tableAttribute];
			sql += "insert into attributes (name, type, noDistinct) " \
				"values (\'" + tableAttributes.name + "\', \'" + to_string(tableAttributes.type) + "\', " + to_string(tableAttributes.noDistinct) + ");";
		}

		conn = sqlite3_exec(db, sql.c_str(), callback, 0, &zErrMsg);
		
		if (conn != SQLITE_OK) {
			fprintf(stderr, "SQL error: %s\n", zErrMsg);
			sqlite3_free(zErrMsg);
		} else {
			fprintf(stdout, "Records created successfully\n");
		}
	}
	

	sqlite3_close(db);

	return true;
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	TablesStruct tab;
	for (auto it = tablesList.begin(); it != tablesList.end(); it++) {
		tab = *it;
		if (tab.name == _table) {
			_noTuples = tab.noTuples;
			return true;
		}
	}
	return false;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
	//Loop through list of tables and delete
	TablesStruct tab;
	for (auto it = tablesList.begin(); it != tablesList.end(); it++) {
		tab = *it;
		if (tab.name == _table) {
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
	// TODO: Check mate
	TablesStruct tab;
	for(auto it = tablesList.begin(); it != tablesList.end(); ++it) {
		tab = *it;
		if(tab.name == _table) {
			int attributeLocation = tab.schema.Index(_attribute);
			tab.schema.GetAtts()[attributeLocation].noDistinct = _noDistinct;
			*it = tab;
		}
	}
}

void Catalog::GetTables(vector<string>& _tables) {
	//TODO: Can we just push_back straight to tables?
	TablesStruct tab;
	vector<string> tables;
	for(auto it = tablesList.begin(); it != tablesList.end(); ++it) {
		tab = *it;
		tables.push_back(tab.name);
	}
	_tables = tables;
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
	for (auto it = tablesList.begin(); it != tablesList.end(); ++it) {
		tab = *it;
		if (tab.name == _table) {
			_schema = tab.schema;
			return true;
		}
	}
	return false;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes, vector<string>& _attributeTypes) {
	/*
	int tablesCounter = 0;
	for(int i = 0; i < tablesList.size(); ++i) {
		tablesCounter++;
	}
	*/
	TablesStruct tab;
	for(int i = 0; i < _attributeTypes.size(); ++i) {
		if(_attributeTypes[i] != "Integer" || _attributeTypes[i] != "Float" || _attributeTypes[i] != "String") {
			fprintf(stdout, "\nAttribute type incorrect.\n");
			return false;
		}
	}

	//Check for duplicate tables
	for(auto it = tablesList.begin(); it != tablesList.end(); ++it) {
		tab = *it;
		if(tab.name == _table) {
			fprintf(stdout, "\nDuplicate Table\n");
			return false;
		}
	}

	//Check for duplicate attributes
	for(auto it = tablesList.begin(); it != tablesList.end(); it++) {
		tab = *it;
		for(int i = 0; i < tab.schema.GetAtts().size(); ++i) {
			if(_attributes[i] == tab.schema.GetAtts()[i].name) {
				fprintf(stdout, "\n Duplicate Attribute Found\n");
				return false;
			}
		}			
	}


	//Add new table
	TablesStruct newTable;
	
	newTable.name = _table;
	newTable.noTuples = 0;
	newTable.pathToFile="NoPath";
	//Have to list through total atts for the table
	for(int i = 0; i < _attributes.size(); ++i) {

		noDistinct.push_back(0); //For schema assigning
	}
	
    Schema newSchema(_attributes, _attributeTypes, noDistinct);

	newTable.schema = newSchema;
	tablesList.push_back(newTable);
	//schemaList.push_back(Schema(_attributes, _attributeTypes, noDistinct));
	return true;
}

bool Catalog::DropTable(string& _table) {
	// Loop through list of tables and delete
	TablesStruct tab;
		//Cannot find tables
	if(tablesHash.find(_table) != tablesHash.end()) { // Tables does not exist
		fprintf(stdout, "\nCannot find table\n");
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
	TablesStruct tab;
	// Loop through all tables. 
	//for(int i = 0; i < tablesList.size(); ++i) {
	for(auto it = tablesList.begin(); it != tablesList.end(); ++it) {
		tab = *it;
		//Print out tables with specified format
		printf("Path : %s\t %u\t %s \t", tab.name.c_str(), tab.noTuples, tab.pathToFile.c_str());
			//Loop through attributes for a tables
			for(int i = 0; i < tab.schema.GetNumAtts(); ++i) {
				//string is needed. cannot get a type working in printf
				if(tab.schema.GetAtts()[i].type == Integer) {
					typeConvert = "Integer";
				}
				else if(tab.schema.GetAtts()[i].type == Float) {
					typeConvert = "Float";
				}
				else if(tab.schema.GetAtts()[i].type == String) {
					typeConvert = "String";
				}
				//Print out attributes with specified format
				printf("Path : %s\t %s\t %u\t", tab.schema.GetAtts()[i].name.c_str(), typeConvert.c_str(), tab.schema.GetAtts()[i].noDistinct);
			}
	}
	return _os;
}
