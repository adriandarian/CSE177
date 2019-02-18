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

// 2-4pm Tuesdays for TA se2 first floor or 313
// 3-5pm Friday for Professor se2 210

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
sqlite3_stmt *stmt;
sqlite3_stmt *res;
char *zErrMsg = 0;
int conn;
int rc;
const char* d = "Callback function called";
string sql;
string typeString;

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
	//conn = sqlite3_exec(db, sql.c_str(), callback, (void*)d, &zErrMsg);
  conn = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, 0);    
	
	if (conn != SQLITE_OK) {
		fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(db));
		sqlite3_close(db);
	}    

	TablesStruct tab;                                                             

	while ((conn = sqlite3_step(stmt)) == SQLITE_ROW) {                                              
		printf("%s | %s | %d\n", sqlite3_column_text(stmt, 0), sqlite3_column_text(stmt, 1), sqlite3_column_int(stmt, 2)); 
		
		tab.name = reinterpret_cast<const char*> (sqlite3_column_text(stmt, 0));
		tab.pathToFile = reinterpret_cast<const char*> (sqlite3_column_text(stmt, 1));
		tab.noTuples = sqlite3_column_int(stmt, 2);

		sql = "select name, type, noDistinct, table_name from attributes";
		rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &res, 0);

		if (rc != SQLITE_OK) {
			fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(db));
			sqlite3_close(db);
		}

		while ((rc = sqlite3_step(res)) == SQLITE_ROW) {
			printf("%s | %s | %d | %s\n", sqlite3_column_text(res, 0), sqlite3_column_text(res, 1), sqlite3_column_int(res, 2), sqlite3_column_text(res, 3)); 
			if (tab.name == reinterpret_cast<const char*> (sqlite3_column_text(res, 3))) {
				for (auto i = 0; i < tab.schema.GetAtts().size(); i++) {
					tab.schema.GetAtts()[i].name = reinterpret_cast<const char*> (sqlite3_column_text(res, 0));
					// tab.schema.GetAtts()[i].type = reinterpret_cast<Type> (sqlite3_column_text(res, 1));
					tab.schema.GetAtts()[i].noDistinct = sqlite3_column_int(res, 2);
				}
			}
		}


	}
	sqlite3_finalize(stmt);
	

	

	

	sqlite3_close(db);
}

Catalog::~Catalog() {
	// Save();
	// tablesList.clear();
}

string createQuotes( const string& s ) {
    return string("\'") + s + string("\'");
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
		string sqlTest = "insert into tables(name, pathTofile, noTuple) values (?,?,?);";
		conn = sqlite3_prepare_v2(db, sqlTest.c_str(), -1, &stmt, 0);
		string sql = "insert into tables (name, pathToFile, noTuples) " \
			"values (" + createQuotes(tab.name) + "," + createQuotes(tab.pathToFile) + "," + createQuotes(to_string(tab.noTuples)) + ");";
		for (auto tableAttribute = 0; tableAttribute < tab.schema.GetAtts().size(); tableAttribute++) {
			auto tableAttributes = tab.schema.GetAtts()[tableAttribute];
			
			//Type conversion 
			if(tableAttributes.type == Integer) {
				typeString = "Integer";
			}
			else if (tableAttributes.type == Float){
				typeString = "Float";
			}
			else if (tableAttributes.type == String) {
				typeString = "String";
			}
			
			sql += "insert into attributes (name, type, noDistinct, table_name) " \
				"values (" + createQuotes(tableAttributes.name) + "," + createQuotes(to_string(tableAttributes.type)) + ","+ createQuotes(to_string(tableAttributes.noDistinct)) + "," + createQuotes(tab.name) + ");";
		} //Attribute For
		conn = sqlite3_exec(db, sql.c_str(), callback, 0, &zErrMsg);
		if (conn != SQLITE_OK) {
			fprintf(stderr, "SQL error: %s\n", zErrMsg);
			sqlite3_free(zErrMsg);
		} else {
			fprintf(stdout, "Record created successfully\n");
		}
	}// Table For		

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
	for(int i = 0; i < tablesList.size(); ++i) {
		if(tablesList[i].name == _table) {
			tablesList[i].noTuples = _noTuples;
		}
	}
	// for (auto it = tablesList.begin(); it != tablesList.end(); it++) {
	// 	tab = *it;
	// 	if (tab.name == _table) {
	// 		tab.noTuples = _noTuples;
	// 		*it = tab;
	// 	}
	// }
	
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
	for(int i = 0; i < tablesList.size(); ++i) {
		if(tablesList[i].name == _table) {
			tablesList[i].pathToFile = _path;
		}
	}
	// TablesStruct tab;
	// for(auto it = tablesList.begin(); it != tablesList.end(); it++) {
	// 	tab = *it;
	// 	if(tab.name == _table) {
	// 		tab.pathToFile = _path;
	// 		*it = tab;
	// 	}
	// }
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute, unsigned int& _noDistinct) {
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
	for(int i = 0; i < tablesList.size(); ++i) {
		if(tablesList[i].name == _table) {
			for(int j = 0; j < tablesList[i].schema.GetAtts().size(); ++j) {
				if(tablesList[i].schema.GetAtts()[j].name == _attribute) 
					tablesList[i].schema.GetAtts()[j].noDistinct == _noDistinct;
				}
		}
	}
	
	// TablesStruct tab;
	// for(auto it = tablesList.begin(); it != tablesList.end(); ++it) {
	// 	tab = *it;
	// 	if(tab.name == _table) {
	// 		int attributeLocation = tab.schema.Index(_attribute);
	// 		tab.schema.GetAtts()[attributeLocation].noDistinct = _noDistinct;
	// 		*it = tab;
	// 	}
	// }
}

void Catalog::GetTables(vector<string>& _tables) {
	TablesStruct tab;
	for (auto it = tablesList.begin(); it != tablesList.end(); ++it) {
		tab = *it;
		_tables.push_back(tab.name);
	}
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	TablesStruct tab;
	for(auto it = tablesList.begin(); it != tablesList.end(); it++) {
		tab = *it;
		if(tab.name == _table) {
			for(int i = 0; i < tab.schema.GetAtts().size(); ++i) {
				_attributes.push_back(tab.schema.GetAtts()[i].name);
			}			
			return true;
		}
	}
	return false;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
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
	//Check for duplicate attributes
	for(int i = 0; i < _attributes.size(); i++) {
		for(int j = 0; j < _attributes.size(); j++) {
			if((i != j) && (_attributes[i] == _attributes[j])) {
	 			fprintf(stdout, "\nDuplicate Attribute\n");
	 			return false;
	 		}
	 	}
	}

	// Add new table_attributes.size()
	TablesStruct tab;
	//noDistinct.clear();
	if(tablesList.size() == 0) {
		tab.name = _table;
		tab.noTuples = 0;
		tab.pathToFile = "NoPath";
		for (int i = 0; i < _attributes.size(); ++i) {
			noDistinct.push_back(0); // For schema assigning
		}
		Schema newSchemaB(_attributes, _attributeTypes, noDistinct);
		tab.schema = newSchemaB;
		tablesList.push_back(tab);
	} else {
		for (auto it = tablesList.begin(); it != tablesList.end(); ++it) {
			tab = *it;
			if (tab.name == _table) {
				printf("Duplicate Table: %s\n", _table.c_str());
				return false;
			} else {
				// Initialize Table
				tab.name = _table;
				tab.noTuples = 0;
				tab.pathToFile = "NoPath";
				// Initialize Attributes
				for (int i = 0; i < _attributes.size(); ++i) {
					noDistinct.push_back(0); // For schema assigning
				}
				Schema newSchema(_attributes, _attributeTypes, noDistinct);
				tab.schema = newSchema;
				
			}
		}
		tablesList.push_back(tab);

	}


	//fprintf(stdout, "TablesList size: %d\n", tablesList.size());

	return true;
}

bool Catalog::DropTable(string& _table) {
	// Loop through list of tables and delete
	TablesStruct tab;

	//TODO: Maybe loop through tablesList first and see what needs to be deleted. Then do the actual SQL delete afterwards?

	conn = sqlite3_open(file.c_str(), &db);
	for(auto it = tablesList.begin(); it != tablesList.end(); it++) {
		tab = *it;
		string sql = "delete from tables where name = " + createQuotes(_table) + ";";

		for (auto tableAttribute = 0; tableAttribute < tab.schema.GetAtts().size(); tableAttribute++) {
			auto tableAttributes = tab.schema.GetAtts()[tableAttribute];
			if (_table == tab.name) {
				sql += "delete from attributes where name = " + createQuotes(tableAttributes.name) + ";";
			}
		}

		conn = sqlite3_exec(db, sql.c_str(), callback, 0, &zErrMsg);
		
		if (conn != SQLITE_OK) {
			fprintf(stderr, "SQL error: %s\n", zErrMsg);
			sqlite3_free(zErrMsg);
		} else {
			fprintf(stdout, "Record created successfully\n");
		}
	}

	for(auto it = tablesList.begin(); it != tablesList.end();) {
		tab = *it;
		if(tab.name == _table) {
			it = tablesList.erase(it);
		}
		else {
			it++;
			return false;
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
	for(auto it = tablesList.begin(); it != tablesList.end(); ++it) {
		tab = *it;
		//Print out tables with specified format
		printf("Table Output : %s\t %u\t %s \t \n", tab.name.c_str(), tab.noTuples, tab.pathToFile.c_str());
			//Loop through attributes for a tables
			for(int i = 0; i < tab.schema.GetAtts().size(); ++i) {
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
				printf("Attribute Output : %s\t %s\t %u\t \n", tab.schema.GetAtts()[i].name.c_str(), typeConvert.c_str(), tab.schema.GetAtts()[i].noDistinct);
			}
	}
	return _os;
}
