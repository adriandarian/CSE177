#include <iostream>
#include <cstring>
#include <string>
#include <fstream>
#include <ctype.h>
#include <locale>  

#include "Catalog.h"
#include "EfficientMap.h"

using namespace std;

void Catalog::print_error_message() {
	cout << sqlite3_errmsg(db) << endl;
	
}

void Catalog::check_query(string query) {
	rc = sqlite3_prepare_v2(db, query.c_str(), -1, &statement, 0);
	if (rc != SQLITE_OK) {
		cout << "Error Preparing Query: " << endl;
		cout << sqlite3_errstr(rc) << endl;
	}
}

void Catalog::open_database(const char*_filename) {
	rc = sqlite3_open(_filename, &db);
	// Testing if the connection is okay
	if (rc != SQLITE_OK) {
		print_error_message();
	}
	else {
		cout << _filename << " is opened successfully." << endl;
	}
}

void Catalog::close_database() {
	sqlite3_close(db);
}

void Catalog::get_data_from_meta_tables() {
	//getting data from MetaTables
	string sql = "SELECT * FROM meta_tables;";
	check_query(sql);

	rc = sqlite3_exec(db, "BEGIN TRANSACTION", 0, 0, 0);

	rc = sqlite3_step(statement);

	while (rc != SQLITE_DONE) {
		TableInfo data_to_push_into_map;

		const char*t_name = reinterpret_cast<const char *>(sqlite3_column_text(statement, 0));
		int t_number_of_tuples = sqlite3_column_int(statement, 1);
		const char*t_datafile_location = reinterpret_cast<const char *>(sqlite3_column_text(statement, 2));
		
		//data in the specified format Name| number of tuples | datefile location
		data_to_push_into_map.set_name(t_name);
		data_to_push_into_map.set_number_of_tuples(t_number_of_tuples);
		data_to_push_into_map.set_data_path(t_datafile_location);

		//key for the data for the hash map
		KeyString data_key(data_to_push_into_map.get_name());

		tables.Insert(data_key, data_to_push_into_map);
		rc = sqlite3_step(statement);
		
	}
	
	rc = sqlite3_exec(db, "END TRANSACTION", 0, 0, &zErrMsg);
	rc = sqlite3_reset(statement);
	rc = sqlite3_finalize(statement);

}

void Catalog::get_data_from_meta_attributes() {
	//getting data from MetaTables
	string sql = "SELECT * FROM meta_attributes";
	check_query(sql);

	rc = sqlite3_exec(db, "BEGIN TRANSACTION", 0, 0, 0);

	rc = sqlite3_step(statement);

	while (rc != SQLITE_DONE) {
		
		vector<string> attributes;
		vector<string> attribute_types;
		vector<unsigned int> number_of_distinct_values;

		KeyString t_name(reinterpret_cast<const char *>(sqlite3_column_text(statement, 0)));

		const char*a_name = reinterpret_cast<const char *>(sqlite3_column_text(statement, 1));
		attributes.push_back(a_name);
		
		const char*a_type = reinterpret_cast<const char*>(sqlite3_column_text(statement, 2));
		attribute_types.push_back(a_type);
		
		int a_number_of_distinct_values = reinterpret_cast<int>(sqlite3_column_int(statement, 3));
		number_of_distinct_values.push_back(a_number_of_distinct_values);

	
		rc = sqlite3_step(statement);

		if (tables.IsThere(t_name)) {
			TableInfo &table_for_schema = tables.Find(t_name);
			//Schema currentSchema = table_for_schema.get_Schema();
			Schema to_push(attributes, attribute_types , number_of_distinct_values);
			table_for_schema.set_Schema(to_push);
			
		}

		
		attributes.clear();
		attribute_types.clear();
		number_of_distinct_values.clear();
	}

	
	rc = sqlite3_exec(db, "END TRANSACTION", 0, 0, &zErrMsg);
	rc = sqlite3_reset(statement);
	rc = sqlite3_finalize(statement);
}

Catalog::Catalog(string& _fileName) {
	file_name = _fileName.c_str();

	//opening the connection to the database to access the meta tables
	open_database(file_name);

	get_data_from_meta_tables();
		
	get_data_from_meta_attributes();

	close_database();
}

Catalog::~Catalog() {
	Save();
	tables.Clear();
}

void Catalog::delete_contents_of_database() {
	open_database(file_name);
	string sql = "DELETE FROM meta_tables;";
	check_query(sql);

	rc = sqlite3_exec(db, "BEGIN TRANSACTION", 0, 0, 0);

	rc = sqlite3_step(statement);

	rc = sqlite3_exec(db, "END TRANSACTION", 0, 0, &zErrMsg);
	rc = sqlite3_reset(statement);
	rc = sqlite3_finalize(statement);


	string sql2 = "DELETE FROM meta_attributes;";
	check_query(sql2);

	rc = sqlite3_exec(db, "BEGIN TRANSACTION", 0, 0, 0);

	rc = sqlite3_step(statement);

	rc = sqlite3_exec(db, "END TRANSACTION", 0, 0, &zErrMsg);
	rc = sqlite3_reset(statement);
	rc = sqlite3_finalize(statement);

	close_database();
}

bool Catalog::Save() {
	delete_contents_of_database();
	open_database(file_name);

	tables.MoveToStart();
	
	cout << "-----------------Begin Saving---------------------------------------------------------------------------";
	cout << endl;

	rc = sqlite3_exec(db, "BEGIN TRANSACTION", 0, 0, 0);
	while (!tables.AtEnd()) {
		string sql = "INSERT INTO meta_tables(t_name, t_number_of_tuples, t_datafile_location) VALUES (?,?,?);";
		check_query(sql);
		
		rc = sqlite3_bind_text(statement, 1, tables.CurrentData().get_name().c_str(), -1, NULL);
		rc = sqlite3_bind_int(statement, 2, tables.CurrentData().get_number_of_tuples());
		rc = sqlite3_bind_text(statement, 3, tables.CurrentData().get_data_path().c_str(), -1, NULL);

		rc = sqlite3_step(statement);
		//rc = sqlite3_finalize(statement);

		vector<Attribute> attributes = tables.CurrentData().get_Schema().GetAtts();
		for (int i = 0; i < attributes.size(); i++) {
			string sql = "INSERT INTO meta_attributes(t_name, a_name, a_type, a_number_of_distinct_values) VALUES (?,?,?,?);";
			check_query(sql);

			rc = sqlite3_bind_text(statement, 1, tables.CurrentData().get_name().c_str(), -1, NULL);
			rc = sqlite3_bind_text(statement, 2, attributes[i].name.c_str(), -1, NULL);
			rc = sqlite3_bind_text(statement, 3, (tables.CurrentData().convertType(attributes[i].type)).c_str(), -1, NULL);
			rc = sqlite3_bind_int(statement, 4, attributes[i].noDistinct);

			rc = sqlite3_step(statement);
			//rc = sqlite3_finalize(statement);
		}

		tables.Advance();
	}
	rc = sqlite3_exec(db, "END TRANSACTION", 0, 0, &zErrMsg);
	tables.MoveToStart();
	close_database();
	cout << "-----------------Done Saving----------------------------------------------------------------------------" << endl;
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	KeyString tableKey(_table);

	if (tables.IsThere(tableKey) == 0) {
		cout << "Error: " << _table << " not found!" << endl;
		return false;
	}
	else {
		_noTuples = tables.Find(tableKey).get_number_of_tuples();
		return true;
	}
	
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
	KeyString tableKey(_table);

	if (tables.IsThere(tableKey) == 0) {
		cout << "Error: " << _table << " not found!" << endl;
		return;
	}
	else {
		tables.Find(tableKey).set_number_of_tuples(_noTuples);
		
	}
	
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	KeyString tableKey(_table);

	if (tables.IsThere(tableKey) == 0) {
		cout << "Error: " << _table << " not found!" << endl;
		return false;
	}

		_path = tables.Find(tableKey).get_data_path();
		return true;
	
}

void Catalog::SetDataFile(string& _table, string& _path) {
	KeyString tableKey(_table);

	if (tables.IsThere(tableKey) == 0) {
		cout << "Error: " << _table << " not found!" << endl;
		return;
	}
	
		tables.Find(tableKey).set_data_path(_path);
	
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	KeyString tableKey(_table);

	if (tables.IsThere(tableKey) == 0) {
		cout << "Error: " << _table << " not found!" << endl;
		return false;
	}
	
		_noDistinct = tables.Find(tableKey).get_Schema().GetDistincts(_attribute);
		
		return true;
	
}

void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	KeyString tableKey(_table);

	if (tables.IsThere(tableKey) == 0) {
		cout << "Error: " << _table << " not found!" << endl;
		return;
	}
	else {
		int findAttributeIndex = tables.Find(tableKey).get_Schema().Index(_attribute);
		tables.Find(tableKey).get_Schema().GetAtts()[findAttributeIndex].noDistinct = _noDistinct;
	
	}
}

void Catalog::GetTables(vector<string>& _tables) {
	tables.MoveToStart();
	while (!tables.AtEnd()) {
		_tables.push_back(tables.CurrentData().get_name());
		tables.Advance();
	}
	tables.MoveToStart();
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	
	KeyString tableKey(_table);

	if (tables.IsThere(tableKey) == 0) {
		cout << "Error: " << _table << " not found!" << endl;
		return false;
	}
	else {
		vector<Attribute> attributesFromCatalog = tables.Find(tableKey).get_Schema().GetAtts();
		for (int i = 0; i < attributesFromCatalog.size(); i++) {
			_attributes.push_back(attributesFromCatalog[i].name);
		}
		return true;
	}
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	
	KeyString tableKey(_table);

	if (tables.IsThere(tableKey) == 0) {
		cout << "Error: " << _table << " not found!" << endl;
		return false;
	}
	else {
		_schema = tables.Find(tableKey).get_Schema();
		return true;
	}
	
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {
	
	KeyString table_key(_table);
	vector<unsigned int> number_of_distinct_values;
	int number_of_tuples = 0;
	string dataFileLocation = "../data/";
	dataFileLocation += _table;
	dataFileLocation += ".bin";

	if (tables.IsThere(table_key) == 1) {
		cout << "Sorry the table '" << _table <<  "' already Exists" << endl;
		return false;
	}
	
		for (int i = 0; i < _attributes.size(); i++) {
			number_of_distinct_values.push_back(0);
		}


		Schema attributes(_attributes, _attributeTypes, number_of_distinct_values);

		TableInfo table_data;

		table_data.set_name(_table);
		table_data.set_data_path(dataFileLocation);
		table_data.set_number_of_tuples(number_of_tuples);
		table_data.set_Schema(attributes);

		tables.Insert(table_key, table_data);

		ofstream createNewFile;
		createNewFile.open(dataFileLocation.c_str(), ios::binary);
		if (createNewFile.is_open())
		{	
			cout << "File was opened" << endl;
			createNewFile.close();
		}
	
		return true;
	
}

bool Catalog::DropTable(string& _table) {
	KeyString table_key(_table);
	if (tables.IsThere(table_key) == 0) {
		cout << "Sorry the table '" << _table << "' does not Exist" << endl;
		return false;
	}
	
		KeyString removed_key;
		TableInfo table_data;

		tables.Remove(table_key, removed_key, table_data);

		return true;
	

}

ostream& operator<<(ostream& _os, Catalog& _c) {
	_os << "-------------------------Printing Catalog-------------------------------------------------------------------";
	cout << endl;

	_c.tables.MoveToStart();

	while (!_c.tables.AtEnd()) {
		cout << "Table Name: " << _c.tables.CurrentData().get_name() << "\t";
		cout << "Number of Tuples: " << _c.tables.CurrentData().get_number_of_tuples() << "\t";
		cout << "Datafile Location: " << _c.tables.CurrentData().get_data_path() << endl;
		

		vector<Attribute> attributes = _c.tables.CurrentData().get_Schema().GetAtts();
		if (attributes.empty()) {
			cout << "Nothing inside this table for the schema" << endl;
		}
		else {
			for (int i = 0; i < attributes.size(); i++) {
				cout << "Attribute Name: " << attributes[i].name << "\t";
				cout << "Attribute Type: " << _c.tables.CurrentData().convertType(attributes[i].type) << "\t";
				cout << "Number of Distinct Values: " << attributes[i].noDistinct << endl;
			}
		}

		_c.tables.Advance();

	}

	_c.tables.MoveToStart();

	return _os;
}
