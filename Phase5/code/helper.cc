#include <iostream>
#include "DBFile.h"
#include "Catalog.h"
#include "Schema.h"
#include <string.h>
using namespace std;

int main(){
	string db = "catalog.sqlite";
	Catalog catalog(db);	
	cout << catalog << endl;
	string comm;
	DBFile dbfile;
	string sname;
	string hname;
	string dname;	
	while (comm != "exit"){
		cout << "Enter the schema name" << endl;
		cin >> sname;
		cout << "Enter the heap file name (.txt files?)" << endl;
		cin >> hname;
		cout << "Enter the data file name (.tbl files)" << endl;
		cin >> dname;
		char* schar = new char[sname.length()+1];
		strcpy(schar,sname.c_str());		
		char* hfile = new char[hname.length()+1];
		strcpy(hfile,hname.c_str());
		char* dfile = new char[dname.length()+1];
		strcpy(dfile,dname.c_str());
		int created = dbfile.Create(hfile,Heap);
		//cout << created << endl;
		Schema schema;
		if(!catalog.GetSchema(sname,schema)){
			cout << "nada" << endl;
			exit(-1);
		}
		//cout << got << endl;		
		dbfile.Load(schema,dfile);
		cout << "loaded" << endl;
		dbfile.Close();

		catalog.SetDataFile(sname,hname);
		string check;
		catalog.GetDataFile(sname,check);
		delete[] schar;
		delete[] dfile;
		delete[] hfile;
		cout << "exit?" <<endl;
		cin >> comm;
	}
	cout << catalog << endl;
}