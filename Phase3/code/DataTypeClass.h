#ifndef _DATA_TYPE_CLASS_H
#define _DATA_TYPE_CLASS_H

#include <iostream>
#include <cstring>
#include <string>
#include "Schema.h"
#include "EfficientMap.h"
#include "Swap.h"


using namespace std;

class TableInfo {
	private:
		string t_name;
		string t_data_path;
		int t_number_of_tuples;
		Schema list_of_attributes;

	public: 
		TableInfo();
		TableInfo(string table_name, int number_of_tuples, string data_path);
		void set_name(string table_name);
		void set_number_of_tuples(int number_of_tuples);
		void set_data_path(string data_path);
		void set_Schema(const Schema& _other);
		void Swap(TableInfo& withMe);
		void CopyFrom(TableInfo& withMe);

		string& get_name();
		int& get_number_of_tuples();
		string& get_data_path();
		Schema& get_Schema();
		string convertType(Type tempType);

};

#endif //_DATA_TYPE_CLASS_H