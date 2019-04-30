#include <iostream>
#include "sqlite3.h"

#include <vector>
#include "string.h"
#include "Schema.h"
#include "Catalog.h"

using namespace std;

//print stuff
static int callback(void *data, int argc, char **argv, char **azColName){
   int i;
   fprintf(stderr, "%s: ", (const char*)data);

   for(i = 0; i<argc; i++){
      printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
   }

   printf("\n");
   return 0;
}

Catalog::Catalog(string& _fileName) {
	string tempTblName;

	const char* data = "Callback function called";

        const char* fname = _fileName.c_str();
        rc = sqlite3_open(fname, &db);

        if (rc) {
                fprintf(stderr, "Failed to open database: %s\n",sqlite3_errmsg(db));
        } else {
               // fprintf(stderr,  "Opened database success\n");

		//////////////////////////////////////////////////////////////
		// Check if table is present in database

		// Execute SQL query for all the data present inside database
		string sql = "SELECT name, type, numDistinct, tblName, tname, numtuples, fileLocation FROM attributeT, tablA WHERE tblName = tname";
//		string sql1 = "SELECT * FROM sqlite_master";

//		rc = sqlite3_exec(db, sql1.c_str(), callback, (void*)data, &zErrMsg);
//		rc = sqlite3_exec(db, sql.c_str(), callback, (void*)data, &zErrMsg);
/*
		if( rc != SQLITE_OK ) {
	      		fprintf(stderr, "SQL error: %s\n", zErrMsg);
	      		sqlite3_free(zErrMsg);
	   	} else {
	      		fprintf(stdout, "Operation done successfully\n");
	   	}
*/
//		sqlite3_stmt* statement;
		rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &statement, 0);

		sqlite3_exec(db, "BEGIN", 0, 0, 0);

		if (rc != SQLITE_OK) {
			fprintf(stderr, "Not valid statement%s\n", sqlite3_errmsg(db));
		} else {
			//fprintf(stdout, "Valid\n");
		}

		while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
			// Table's table
			string t_name = reinterpret_cast<const char*>(sqlite3_column_text(statement, 4));

			//  Pushes new values when new table name
			if (t_name != tempTblName) {
	                        tempTblName = t_name;
				unsigned int  numTups = sqlite3_column_int(statement, 5);
				string filePosition = reinterpret_cast<const char*> (sqlite3_column_text(statement, 6));

				SchemaName.push_back(tempTblName);
				SchemaTuples.push_back(numTups);
				SchemaPosition.push_back(filePosition);
			}

			// Table's attribute
			string attsName = reinterpret_cast<const char*> (sqlite3_column_text(statement, 0));
			string attsType = reinterpret_cast<const char*> (sqlite3_column_text(statement, 1));
			unsigned int numDistinct = sqlite3_column_int(statement, 2);
			string attsTable = reinterpret_cast<const char*> (sqlite3_column_text(statement,3));

			// Store all the table's attributes
			attribute tempAtts;
			tempAtts.name = attsName;
			tempAtts.type = attsType;
			tempAtts.numDistinct = numDistinct;
			tempAtts.tblName = attsTable;

			attributes.push_back(tempAtts);

		}

		sqlite3_exec(db, "COMMIT", 0, 0, 0);

		sqlite3_finalize(statement);
		if (rc != SQLITE_DONE) {
			fprintf(stderr, "SQL error: %s\n", zErrMsg);
			sqlite3_free(zErrMsg);
		} else {
			// grouping data by table's name
			for (int i = 0; i < SchemaName.size(); i++) {
				vector<string> a_name;
				vector<string> a_type;
				vector<unsigned int> a_numDistinct;

				for (int j = 0; j < attributes.size(); j++) {
					if  (attributes[j].tblName == SchemaName[i]) {
						a_name.push_back(attributes[j].name);
	                                        a_type.push_back(attributes[j].type);
	                                        a_numDistinct.push_back(attributes[j].numDistinct);
					}
				}

				// new schema
				if (!a_name.empty()) {
					schema.push_back(Schema(a_name, a_type, a_numDistinct));
				}
			}
			//fprintf(stdout, "Table loaded\n");
		}
	}
}


Catalog::~Catalog() {
	//Save before exit
	Save();
	sqlite3_close(db);
}

bool Catalog::Save() {
	string sql;
        int tableSize = SchemaName.size();
        int attSize = attributes.size();

//DROP table  Attribute
	sql = "DROP TABLE attributeT";
	rc = sqlite3_exec(db, sql.c_str(),NULL,0, &zErrMsg);
	sqlite3_exec(db, "BEGIN", 0, 0, 0);

	if(rc != SQLITE_OK){
		fprintf(stderr,"DROP  TABLE failed %s\n",zErrMsg);
		sqlite3_free(zErrMsg);
	}

//DROP table Table
        sqlite3_exec(db, "COMMIT", 0, 0, 0);

	sql = "DROP TABLE tablA";
	rc = sqlite3_exec(db, sql.c_str(),NULL,0, &zErrMsg);
        sqlite3_exec(db, "BEGIN", 0, 0, 0);

	if(rc  != SQLITE_OK){
		fprintf(stderr,"DROP TABLE failed %s\n",zErrMsg);
		sqlite3_free(zErrMsg);
	}
//CREATE NEW TABLE TO INSERT UPDATED DATA
	sqlite3_exec(db, "COMMIT", 0, 0, 0);

	sql = "CREATE TABLE tablA (tname STRING NOT NULL, numTuples INTEGER NOT NULL, fileLocation VARCHAR(100) NOT NULL)";
	rc = sqlite3_exec(db, sql.c_str(),NULL,0, &zErrMsg);
        sqlite3_exec(db, "BEGIN", 0, 0, 0);

	if(rc!= SQLITE_OK){
		fprintf(stderr,"CREATE TABLE failed %s\n",zErrMsg);
		sqlite3_free(zErrMsg);
	}

//CREATE NEW TABLE FOR ATTRIBUTE
	sqlite3_exec(db, "COMMIT", 0, 0, 0);

	sql = "CREATE TABLE attributeT (name STRING NOT NULL, type STRING NOT NULL, numDistinct INTEGER NOT NULL, tblName STRING NOT NULL)";
	rc = sqlite3_exec(db, sql.c_str(),NULL,0, &zErrMsg);
        sqlite3_exec(db, "BEGIN", 0, 0, 0);

	if(rc!= SQLITE_OK){
		fprintf(stderr,"CREATE TABLE FAILED %s\n",zErrMsg);
		sqlite3_free(zErrMsg);
	}

//Prepared Statement to Insert
        sqlite3_exec(db, "COMMIT", 0, 0, 0);
	string insertAtt = "INSERT INTO attributeT VALUES(?1,?2,?3,?4);";
	string insertTbl = "INSERT INTO tablA VALUES(?1,?2, ?3);";

//tablA
	rc = sqlite3_prepare_v2(db, insertTbl.c_str(),-1,&statement,0);
        sqlite3_exec(db, "BEGIN", 0, 0, 0);

//sizes
	for(int i =0; i < tableSize;i++){
		sqlite3_reset(statement);
		//Name
		rc = sqlite3_bind_text( statement, 1, SchemaName[i].c_str(),SchemaName[i].size(),SQLITE_STATIC);
		//numTup
		rc = sqlite3_bind_int( statement, 2, SchemaTuples[i]);
		//file Loc
		rc = sqlite3_bind_text( statement, 3, SchemaPosition[i].c_str(), SchemaPosition[i].size(), SQLITE_STATIC);
		rc = sqlite3_step(statement);

		if(rc != SQLITE_DONE){

			fprintf(stderr,"ERROR %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_exec(db, "COMMIT", 0, 0, 0);

	sqlite3_finalize(statement);

//atty
	rc = sqlite3_prepare_v2(db, insertAtt.c_str(), -1, &statement,0);
	sqlite3_exec(db, "BEGIN", 0, 0, 0);

	for(int i =0; i < attSize; i++){
		sqlite3_reset(statement);

		rc = sqlite3_bind_text( statement, 1, attributes[i].name.c_str(), attributes[i].name.size(), SQLITE_STATIC);

		rc = sqlite3_bind_text( statement, 2, attributes[i].type.c_str(), attributes[i].type.size(), SQLITE_STATIC);

		rc = sqlite3_bind_int(statement, 3 , attributes[i].numDistinct);

		rc = sqlite3_bind_text(statement, 4, attributes[i].tblName.c_str(), attributes[i].tblName.size(), SQLITE_STATIC);

		rc = sqlite3_step(statement);

		if(rc != SQLITE_DONE){
			fprintf(stderr, "ERROR %s\n",  sqlite3_errmsg(db));
		}
	}
	sqlite3_exec(db, "COMMIT", 0, 0, 0);
	sqlite3_finalize(statement);
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	int tableSize = SchemaName.size();

	for (int  i = 0; i < tableSize; i++)
	{
		if (_table == SchemaName[i])
		{
                       _noTuples =  SchemaTuples[i];
			return true;
		}
	}

	return false;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
	int tableSize = SchemaName.size();

	for (int i = 0; i < tableSize; i++)
	{
		if (_table == SchemaName[i])
		{
			SchemaTuples[i] = _noTuples;
		}
	}
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	int tableSize = SchemaName.size();

	for (int i =- 0; i < tableSize; i++)
	{
		if (_table == SchemaName[i])
		{
			_path = SchemaPosition[i];
			return true;
		}
	}

	return false;
}

void Catalog::SetDataFile(string& _table, string& _path) {
	int tableSize = SchemaName.size();

	for (int  i = 0; i < tableSize; i++)
	{
		if (_table == SchemaName[i])
		{
			SchemaPosition[i] = _path;
		}
	}
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
//	int tableSize = SchemaName.size();

        int attSize = attributes.size();

        for (int i = 0; i < attSize; i++) {
                if (attributes[i].name == _attribute) {
//                        attributes[i].numDistinct = _noDistinct;
			_noDistinct = attributes[i].numDistinct;
                }
        }
/*
	for (int i = 0; i < tableSize; i++)
	{
		unsigned int schemaDistinct = schema[i].GetDistincts(_attribute);
		if (_table == SchemaName[i])
		{
                        _noDistinct = schemaDistinct;
			return true;
		}
	}
*/
	return false;
}

void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	int attSize = attributes.size();

	for (int i = 0; i < attSize; i++) {
		if (attributes[i].name == _attribute) {
			attributes[i].numDistinct = _noDistinct;
		}
	}
/*
	int tableSize = SchemaName.size();
	int dummyInt;

	for (int i = 0; i < tableSize; i++)
	{
		int schemaIndex = schema[i].Index(_attribute);
		if ( _table == SchemaName[i])
		{
			dummyInt = schemaIndex;
			schema[i].GetAtts()[dummyInt].noDistinct = _noDistinct;
			attributes[i].numDistinct = _noDistinct;
		}
	}
*/
}

void Catalog::GetTables(vector<string>& _tables) {
	_tables = SchemaName;
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	int tableSize = SchemaName.size();
	vector<string> dummyStr;

	for (int i = 0; i < tableSize; i++)
	{
		if (_table == SchemaName[i])
		{
			int schemaAttSize = schema[i].GetAtts().size();
			for (int j = 0; j < schemaAttSize; j++)
			{
				string schemaAttName = schema[i].GetAtts()[j].name;
				dummyStr.push_back(schemaAttName);
			}
			_attributes = dummyStr;
			return true;
		}
	}

	return false;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	int tableSize = SchemaName.size();

	for (int i = 0; i < tableSize; i++)
	{
		if(_table == SchemaName[i])
		{
			_schema = schema[i];
			return true;
		}
	}

	return false;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {

	int tableSize = SchemaName.size();

	for (int i = 0; i < tableSize; i++) {
		if (SchemaName[i] == _table) {
			cout << "Table name EXIST already" << endl;
			return false;
		}
	}

	if (_attributes.size() == 0 || _attributeTypes.size() == 0) {
		cout << "No Attributes or Types" << endl;
		return false;
	}

/*
	if (_attributeTypes.size() != _attributes.size()) {
		cout << "Num types does not match num attributes" <<endl;
		return false;
	}
*/

	for (int i = 0; i < _attributes.size(); i++) {
//		string holder = _attributes[i];
		for (int j = i+1; j < _attributes.size(); j++) {
			if (_attributes[i] == _attributes[j]) {
				cout << " Duplicate attribute name" <<endl;
				return false;
			}
		}
	}

	for (int i = 0; i < _attributeTypes.size(); i++) {
		if  (! (_attributeTypes[i] == "INTEGER" || _attributeTypes[i] == "FLOAT" || _attributeTypes[i] == "STRING"
			|| _attributeTypes[i] == "integer" || _attributeTypes[i] == "float" || _attributeTypes[i] == "string"
			|| _attributeTypes[i] == "Integer" || _attributeTypes[i] == "Float" || _attributeTypes[i] == "String")) {
			cout << "Attribute type is not one of the following: INTEGER, FLOAT nor STRING" << endl;
			return false;
		}
	}

	SchemaName.push_back(_table);
	//vector<attribute> tempAtts;
	attribute tempAtt;
	vector<unsigned int> numDistinct;

//	SchemaTuples.push_back(0);

	//setting data
	for (int  i = 0; i < _attributes.size(); i++) {
		tempAtt.numDistinct = (0);
		tempAtt.name = _attributes[i];
		tempAtt.type = _attributeTypes[i];
		tempAtt.tblName = _table;
		numDistinct.push_back(0);
		attributes.push_back(tempAtt);
//Push back new atts

		//attributes.push_back(tempAtt);
		SchemaTuples.push_back(0);
//		SchemaTuples[SchemaTuples.size()-1]++;
		SchemaPosition.push_back("N/A");
	}

//	SchemaName.push_back(_table);

/*
	for(int i =0; i < tempAtts.size();i++){
		attributes.push_back(tempAtts[i]);
	}
*/
	schema.push_back(Schema(_attributes, _attributeTypes, numDistinct));

	return true;
}

bool Catalog::DropTable(string& _table) {
	int attSize = attributes.size();
	vector<attribute> dummy;

	// if table differs, push the attributes data 
	for (int i = 0; i < attSize; i++)
	{
		if (_table != attributes[i].tblName)
		{
			dummy.push_back(attributes[i]);
		}
	}
	attributes.clear();
	attributes = dummy;


	int schemaSize = schema.size();
/*
	vector<string> dummy1;
        vector<int> dummy2;
        vector<string> dummy3;
        vector<Schema> dummy4;
*/
	//clearing data
	for (int i = 0; i < schemaSize; i++)
	{
		if (_table == SchemaName[i])
		{
/*
			dummy1.push_back(SchemaName[i]);
                        dummy2.push_back(SchemaTuples[i]);
                        dummy3.push_back(SchemaPosition[i]);
                        dummy4.push_back(schema[i]);
*/
			string dummy1 = SchemaName[schemaSize-1];
			unsigned int dummy2 = SchemaTuples[schemaSize-1];
			string dummy3 = SchemaPosition[schemaSize-1];
			Schema dummy4 = schema[schemaSize-1];

			SchemaName[i] = dummy1;
			SchemaTuples[i] = dummy2;
			SchemaPosition[i] = dummy3;
			schema[i] = dummy4;


			SchemaName.erase(SchemaName.end());
			SchemaTuples.pop_back();
			SchemaPosition.erase(SchemaPosition.end());
			schema.erase(schema.end());

			cout << "Success" << endl;
			return true;
		}
	}
/*
	SchemaName.clear();
        SchemaTuples.clear();
        SchemaPosition.clear();
        schema.clear();

        SchemaName = dummy1;
        SchemaTuples = dummy2;
        SchemaPosition = dummy3;
        schema = dummy4;
*/

	cout << "Drop table failed" << endl;
	return false;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	for (int  i = 0; i < _c.SchemaName.size(); i++)
	{
		fprintf(stdout, "\tTable Name \tNumber of Tuples \tFile Path\n");
		fprintf(stdout, "\t%s \t\t%u \t\t\t%s\n\n", _c.SchemaName[i].c_str(), _c.SchemaTuples[i], _c.SchemaPosition[i].c_str());
		fprintf(stdout, "\tAttribute Name \tType \t\t\tNumber of Distincts\n");

		for (int j = 0; j < _c.attributes.size(); j++)
		{
			if (_c.SchemaName[i] == _c.attributes[j].tblName)
			{
				fprintf(stdout, "\t%s \t\t%s \t\t%u\n", _c.attributes[j].name.c_str(), _c.attributes[j].type.c_str(), _c.attributes[j].numDistinct);
			}
		}
		cout << string(100, '-')  << endl;
	}

	return _os;
}
