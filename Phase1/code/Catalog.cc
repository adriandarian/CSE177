#include <iostream>
#include <unordered_set>
#include "sqlite3.h"

#include "Schema.h"
#include "Catalog.h"

using namespace std;

//Could just have vectors for each instead
/*

vector<string> tablesList;
vector<string> attsList;
vector<string> pathToFile;
vector<int> numTuples;
vector<int> numDistinct;
vector<Schema> schemaList; // Not sure if this is needed
*/
struct attsStruct {
	string name;
	string type;
	int numDistinct;
};

struct tablesStruct {
	string name;
	string pathToFile;
	//attsStruct atts;
	int numTuples;
};

//List of all the tables with corresponding data
struct tablesCatalog {
	vector<string> name;
	vector<string> pathToFile;
	vector<attsStruct> atts;
	vector<int> numTuples;
};
tablesCatalog tablesList;

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
	return true;
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	return true;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	return true;
}

void Catalog::SetDataFile(string& _table, string& _path) {
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute, unsigned int& _noDistinct) {
	return true;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute, unsigned int& _noDistinct) {
}

void Catalog::GetTables(vector<string>& _tables) {
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	return true;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	//Loop through all tables
	//Once _table is found set _schema to corresponding schema to that tables
	//return true;
	//else return false
	return true;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes, vector<string>& _attributeTypes) {
	//Check for a valid attribute type
	//Integer in main-2.cc INTEGER in main.cc
	for(int i = 0; i < _attributeTypes.size(); ++i) {
		if(_attributeTypes[i] != "Integer" || _attributeTypes[i] != "Float" || _attributeTypes[i] != "String")
			return false;
	}

	//Check for duplicate attributes, Return false if duplicate is found
	unordered_set <string> attsHash;
	for(int i = 0; i < _attributes.size(); ++i) {
		if(attsHash.find(_attributes[i]) != attsHash.end()) {
			return false;
		}
		else {
			attsHash.insert(_attributes[i]);
		}
	}
	//Check for duplicate tables
	unordered_set <string> tablesHash;
		if(tablesHash.find(_table) != tablesHash.end()) {
			return false;
		}
		else {
			tablesHash.insert(_table);
		}
	tablesList.name.push_back(_table);
	tablesList.pathToFile.push_back("NoPath");
	//tablesList.atts.push_back(_attributes);
	tablesList.numTuples.push_back(0); 

	return true;
}

bool Catalog::DropTable(string& _table) {
	//Loop through list of tables and delete
	for(auto it = tablesList.name.begin(); it != tablesList.name.end(); ++it) {
		if(tablesList.name == _table) {

		}
	}
	return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	return _os;
}
