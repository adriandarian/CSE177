#include <iostream>
#include "sqlite3.h"
#include <stdio.h>
#include <stdlib.h>
#include "Schema.h"
#include "Catalog.h"

using namespace std;

Catalog::Catalog(string &_fileName)
{
	//Initializing
	zErrMsg = 0;
	error = false;
	//Store sentinel value for now;
	tempN = "sentinel";
	//Attempt to open database
	rc = sqlite3_open(_fileName.c_str(), &db);
	//If rc is false ERROR
	if (rc)
	{
		fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
	}
	else
	{

		//fprintf(stderr, "Opened database successfully\n");
		//SQL string to run pulls all data out of the database
		string selAtt = "SELECT a.name,a.type,a.numDistVal,a.tName,t.numTuples,t.fileLoc FROM attributes a,tables t WHERE a.tName = t.name ORDER BY t.tOrd ,a.aOrd";
		//Making prepared statement
		rc = sqlite3_prepare_v2(db, selAtt.c_str(), -1, &stmt, 0);
		//While the result of the sqlite is a tuple
		sqlite3_exec(db, "BEGIN", 0, 0, 0);
		while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
		{
			//Store the values in some temporary variables
			string name = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
			string type = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1));

			unsigned int numDistinct = sqlite3_column_int(stmt, 2);
			string tName = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 3));
			//If the table name is different, push values to signifiy new table
			if (tName != tempN)
			{
				tempN = tName;
				//cout << tempN << " " <<sqlite3_column_int(stmt,4) << " "<< reinterpret_cast<const char*> (sqlite3_column_text(stmt,5)) <<endl;
				schemaN.push_back(tempN);
				schemaT.push_back(sqlite3_column_int(stmt, 4));
				schemaL.push_back(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 5)));
			}

			//cout << name << " " << type << " " << numDistinct << endl;
			//Store all data from the database here
			tNames.push_back(tName);
			attributes.push_back(name);
			types.push_back(type);
			distincts.push_back(numDistinct);
		}
		sqlite3_exec(db, "COMMIT", 0, 0, 0);
		sqlite3_finalize(stmt);
		//ERROR
		if (rc != SQLITE_DONE)
		{
			cout << rc << endl;
			fprintf(stderr, "SQL error: %s\n", zErrMsg);
			sqlite3_free(zErrMsg);
			error = true;
			printf("Close menu and retry");
		}
		else
		{
			//Using temporary vectors, group data by table name
			for (int i = 0; i < schemaN.size(); i++)
			{
				vector<string> aN;
				vector<string> aT;
				vector<unsigned int> aD;
				//cout << schemaN[i] << " ";
				for (int j = 0; j < attributes.size(); j++)
				{
					//If name from Total vector is equal to the schema name, push values to temporary vectors
					if (tNames[j] == schemaN[i])
					{
						// cout << attributes[j] << " ";
						// cout << types[j] << " ";
						// cout << distincts[j] << endl;
						aN.push_back(attributes[j]);
						aT.push_back(types[j]);
						aD.push_back(distincts[j]);
					}
				}
				//Make a new schema and push it.
				if (!aN.empty())
				{
					schemas.push_back(Schema(aN, aT, aD));
				}
			}
			//printf("Data copied successfully\n");
			error = false;
			//print();
		}
	}
}

// int Catalog::putStuffIn(void* catalog, int argc, char** argv, char ** azColName){

// 	Catalog data = *(Catalog *) (catalog);
// 	string name = argv[0];
// 	string type = argv[1];

// 	unsigned int numDistinct = atoi(argv[2]);
// 	string tName = argv[3];

// 	if(tName != data.tempN){

// 			//data.schemas.push_back(Schema(data.attributes,data.types,data.distincts));
// 			data.schemaN.push_back(data.tempN);

// 			data.schemaT.push_back(atoi(argv[4]));
// 			data.schemaL.push_back(argv[5]);
// 			data.tempN = tName;

// 	}
// 	data.tNames.push_back(tName);
// 	data.attributes.push_back(name);
// 	data.types.push_back(type);
// 	data.distincts.push_back(numDistinct);
// 	//cout << data.tNames.size() << endl;
// 	return 0;

// }
Catalog::~Catalog()
{
	//cout << "Destructor" << endl;
	//Save all data
	Save();
	//Close database
	rc = sqlite3_close_v2(db);
	if (rc == SQLITE_OK)
	{
		//printf("Database closed successfully\n");
	}
	else
	{
		printf("Database failed to close\n");
	}
}

bool Catalog::Save()
{
	//Check for error when copying data earlier in constructor
	if (!error)
	{
		//SQL strings
		string delAllT = "DELETE FROM tables";
		string delAllA = "DELETE FROM attributes";
		string insAllT = "INSERT INTO tables VALUES(?1,?2,?3,?4)";
		string insAllA = "INSERT INTO attributes VALUES(?1,?2,?3,?4,?5)";
		//Simple execute for deletes
		rc = sqlite3_exec(db, delAllT.c_str(), 0, 0, &zErrMsg);
		if (rc != SQLITE_OK)
		{
			fprintf(stderr, "SQL error: %s\n", zErrMsg);
			sqlite3_free(zErrMsg);
		}
		else
		{
			//fprintf(stdout, "tables emptied successfully\n");
		}
		//Simple execute for deletes
		rc = sqlite3_exec(db, delAllA.c_str(), 0, 0, &zErrMsg);
		if (rc != SQLITE_OK)
		{
			fprintf(stderr, "SQL error: %s\n", zErrMsg);
			sqlite3_free(zErrMsg);
		}
		else
		{
			//fprintf(stdout, "attributes emptied successfully\n");
		}
		int tCount = 0;
		//Prepared Statement insert all table data
		rc = sqlite3_prepare_v2(db, insAllT.c_str(), -1, &stmt, 0);
		//Going through every table
		sqlite3_exec(db, "BEGIN", 0, 0, 0);
		for (int i = 0; i < schemaN.size(); i++)
		{
			//reset statements so we can reuse
			sqlite3_reset(stmt);
			//Bind values from vectors to SQL statement
			rc = sqlite3_bind_text(stmt, 1, schemaN[i].c_str(), schemaN[i].size(), SQLITE_STATIC);

			rc = sqlite3_bind_int(stmt, 2, schemaT[i]);

			rc = sqlite3_bind_text(stmt, 3, schemaL[i].c_str(), schemaL[i].size(), SQLITE_STATIC);

			rc = sqlite3_bind_int(stmt, 4, tCount);
			tCount++;
			rc = sqlite3_step(stmt);

			if (rc != SQLITE_DONE)
			{
				printf("ERROR inserting tables data: %s\n", sqlite3_errmsg(db));
			}
		}
		sqlite3_exec(db, "COMMIT", 0, 0, 0);
		sqlite3_finalize(stmt); // Free statement
		int aCount = 0;
		//Prepared statement insert all attribute data
		rc = sqlite3_prepare_v2(db, insAllA.c_str(), -1, &stmt, 0);
		//For all tables
		sqlite3_exec(db, "BEGIN", 0, 0, 0);
		for (int i = 0; i < schemas.size(); i++)
		{
			//Temporary variable to make it shorter
			vector<Attribute> temp = schemas[i].GetAtts();
			//For the list of attributes for that schema
			for (int j = 0; j < temp.size(); j++)
			{
				//reset
				sqlite3_reset(stmt);
				//Bind data
				rc = sqlite3_bind_text(stmt, 1, temp[j].name.c_str(), temp[j].name.size(), SQLITE_TRANSIENT);
				//Have to convert from type to string
				string typeT;
				if (temp[j].type == Integer)
					typeT = "INTEGER";
				else if (temp[j].type == Float)
					typeT = "FLOAT";
				else if (temp[j].type == String)
					typeT = "STRING";
				rc = sqlite3_bind_text(stmt, 2, typeT.c_str(), typeT.size(), SQLITE_STATIC);

				rc = sqlite3_bind_int(stmt, 3, temp[j].noDistinct);
				rc = sqlite3_bind_text(stmt, 4, schemaN[i].c_str(), schemaN[i].size(), SQLITE_STATIC);
				rc = sqlite3_bind_int(stmt, 5, aCount);
				aCount++;
				rc = sqlite3_step(stmt);
				if (rc != SQLITE_DONE)
				{
					printf("%s, %s, %u, %s", temp[j].name.c_str(), typeT.c_str(), temp[j].noDistinct, schemaN[i].c_str());
					printf("ERROR inserting attributes data: %s\n", sqlite3_errmsg(db));
				}
			}
		}
		sqlite3_exec(db, "COMMIT", 0, 0, 0);
		//printf("Database content saved\n");
		sqlite3_finalize(stmt);
	}
	else
	{
		printf("Did not save due to initial data copy error");
		return false;
	}
}

bool Catalog::GetNoTuples(string &_table, unsigned int &_noTuples)
{
	//Obvious linear search
	for (int i = 0; i < schemaN.size(); i++)
	{
		if (schemaN[i] == _table)
		{
			_noTuples = schemaT[i];
			return true;
		}
	}
	printf("%s does not exist, using garbage data\n", _table.c_str());
	return false;
}

void Catalog::SetNoTuples(string &_table, unsigned int &_noTuples)
{
	//Obvious linear search
	for (int i = 0; i < schemaN.size(); i++)
	{
		if (schemaN[i] == _table)
		{
			schemaT[i] = _noTuples;
			return;
		}
	}
	printf("Table %s does not exist\n", _table.c_str());
}

bool Catalog::GetDataFile(string &_table, string &_path)
{
	//Obvious linear search
	//cout << schemaN.size() << endl;
	for (int i = 0; i < schemaN.size(); i++)
	{
		//cout << schemaN[i] << endl;
		if (schemaN[i] == _table)
		{
			_path = schemaL[i];
			return true;
		}
	}
	printf("Table %s does not exist, using garbage datafile\n", _table.c_str());
	return false;
}

void Catalog::SetDataFile(string &_table, string &_path)
{
	//Obvious linear search
	for (int i = 0; i < schemaN.size(); i++)
	{
		if (schemaN[i] == _table)
		{
			schemaL[i] = _path;
			return;
		}
	}
	printf("Table %s does not exist\n", _table.c_str());
}

bool Catalog::GetNoDistinct(string &_table, string &_attribute,
														unsigned int &_noDistinct)
{
	//Linear search for table
	for (int i = 0; i < schemaN.size(); i++)
	{
		if (schemaN[i] == _table)
		{
			//Linear search for attribute
			for (int j = 0; j < schemas[i].GetAtts().size(); j++)
			{
				if (_attribute == schemas[i].GetAtts()[j].name)
				{
					_noDistinct = schemas[i].GetDistincts(_attribute);
					return true;
				}
			}
			printf("Attribute %s does not exist, using garbage data\n", _attribute.c_str());
			return false;
		}
	}
	printf("Table %s does not exist, using garbage data\n", _table.c_str());
	return false;
}
void Catalog::SetNoDistinct(string &_table, string &_attribute,
														unsigned int &_noDistinct)
{
	//Linear search
	for (int i = 0; i < schemaN.size(); i++)
	{
		if (schemaN[i] == _table)
		{
			int temp = schemas[i].Index(_attribute);
			schemas[i].GetAtts()[temp].noDistinct = _noDistinct;
			return;
		}
	}
	printf("Table %s does not exist\n", _table.c_str());
}

void Catalog::GetTables(vector<string> &_tables)
{
	_tables = schemaN;
}

bool Catalog::GetAttributes(string &_table, vector<string> &_attributes)
{
	for (int i = 0; i < schemaN.size(); i++)
	{
		if (schemaN[i] == _table)
		{
			vector<string> temp;
			for (int j = 0; j < schemas[i].GetAtts().size(); j++)
			{
				temp.push_back(schemas[i].GetAtts()[j].name);
			}
			_attributes = temp;
			return true;
		}
	}
	return false;
}

bool Catalog::GetSchema(string &_table, Schema &_schema)
{
	//Linear search
	for (int i = 0; i < schemaN.size(); i++)
	{
		if (schemaN[i] == _table)
		{
			_schema = schemas[i];
			// cout << "------------" << schemaN[i] << "------------"<< endl;
			// for(int j = 0; j < _schema.GetAtts().size();j++){
			// 	cout << _schema.GetAtts()[j].name << endl;
			// }
			// cout << "------------------------------------"<< endl;
			return true;
		}
	}
	printf("Table %s does not exist, using garbage data\n", _table.c_str());
	return false;
}

bool Catalog::CreateTable(string &_table, vector<string> &_attributes,
													vector<string> &_attributeTypes)
{

	if (_attributes.size() == 0)
	{
		printf("0 Attributes given\n");
		return false;
	}
	if (_attributeTypes.size() != _attributes.size())
	{
		printf("Number of types don't match the number of attributes\n");
		return false;
	}
	for (int i = 0; i < schemaN.size(); i++)
	{
		if (schemaN[i] == _table)
		{
			printf("Table with same name %s\n", _table.c_str());
			return false;
		}
	}
	for (int i = 0; i < _attributes.size(); i++)
	{
		for (int j = 0; j < _attributes.size(); j++)
		{
			if (_attributes[i] == _attributes[j] && i != j)
			{
				printf("Attributes with the same name!\n");
				return false;
			}
		}
	}

	for (int i = 0; i < _attributeTypes.size(); i++)
	{
		string temp = _attributeTypes[i];
		for (int j = 0; j < temp.length(); j++)
		{
			temp[j] = tolower(temp[j]);
		}
		if (temp == "integer")
		{
			_attributeTypes[i] = "INTEGER";
		}
		else if (temp == "string")
		{
			_attributeTypes[i] = "STRING";
		}
		else if (temp == "float")
		{
			_attributeTypes[i] = "FLOAT";
		}
		if (_attributeTypes[i] == "INTEGER" || _attributeTypes[i] == "STRING" || _attributeTypes[i] == "FLOAT")
		{
		}
		else
		{
			printf("Types must be INTEGER or STRING or FLOAT\n");
			return false;
		}
	}

	schemaN.push_back(_table);
	vector<unsigned int> _noDistinct;
	for (int i = 0; i < _attributes.size(); i++)
	{
		_noDistinct.push_back(0);
		schemaT.push_back(0);
		schemaL.push_back("Unknown");
	}
	schemas.push_back(Schema(_attributes, _attributeTypes, _noDistinct));
	return true;
}

bool Catalog::DropTable(string &_table)
{
	//Linear searcg for table
	for (int i = 0; i < schemas.size(); i++)
	{
		if (schemaN[i] == _table)
		{
			//Be dumb do this
			//Copy all vectors into temporary ones
			vector<string> sN = schemaN;
			vector<int> sT = schemaT;
			vector<string> sL = schemaL;
			vector<Schema> s = schemas;
			//Clear vectors
			schemaN.clear();
			schemaT.clear();
			schemaL.clear();
			schemas.clear();
			//Refill vectors without the copied value, could avoid this if i used hashtables, or if i just shifted from the removed values
			for (int j = 0; j < sN.size(); j++)
			{
				if (j != i)
				{
					schemaN.push_back(sN[j]);
					schemaT.push_back(sT[j]);
					schemaL.push_back(sL[j]);
					schemas.push_back(s[j]);
				}
			}
			return true;
		}
	}
	printf("%s does not exist\n", _table.c_str());
	return false;
}

ostream &operator<<(ostream &_os, Catalog &_c)
{
	//Print nicely
	for (int i = 0; i < _c.getSchemaN().size(); i++)
	{
		printf("=====================================================\n");
		printf("%s \t %u \t %s\n", _c.getSchemaN()[i].c_str(), _c.getSchemaT()[i], _c.getSchemaL()[i].c_str());
		//Lazy variable
		vector<Attribute> temp = _c.getSchemas()[i].GetAtts();
		for (int j = 0; j < temp.size(); j++)
		{
			string typeT;
			if (temp[j].type == Integer)
				typeT = "INTEGER";
			else if (temp[j].type == Float)
				typeT = "FLOAT ";
			else if (temp[j].type == String)
				typeT = "STRING";
			printf("\t %s \t %s \t %u\n", temp[j].name.c_str(), typeT.c_str(), temp[j].noDistinct);
		}
	}
	if (_c.getSchemaN().size() > 0)
	{
		printf("=====================================================\n");
	}

	return _os;
}
void Catalog::print()
{
	cout << "START PRINT" << endl;
	for (int i = 0; i < getSchemaN().size(); i++)
	{
		fprintf(stdout, "%s \t %u \t %s\n", getSchemaN()[i].c_str(), getSchemaT()[i], getSchemaL()[i].c_str());
		vector<Attribute> temp = getSchemas()[i].GetAtts();
		for (int j = 0; j < temp.size(); j++)
		{
			string typeT;
			if (temp[j].type == Integer)
				typeT = "INTEGER";
			else if (temp[j].type == Float)
				typeT = "FLOAT";
			else if (temp[j].type == String)
				typeT = "STRING";
			fprintf(stdout, "\t %s \t %s \t %u\n", temp[j].name.c_str(), typeT.c_str(), temp[j].noDistinct);
		}
	}
	cout << "END PRINT" << endl;
}
