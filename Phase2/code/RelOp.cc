#include <iostream>
#include "RelOp.h"
#include <string>

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file, string _tblName) {
  schema = _schema;
	file = _file;
  tblName = _tblName;
}

Scan::~Scan() {

}

ostream& Scan::print(ostream& _os) {
	_os << "SCAN [ " << tblName << " ]";
	return _os;
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants, RelationalOp* _producer) {
  schema = _schema;
  predicate = _predicate;
  constants = _constants;
  producer = _producer;
}

Select::~Select() {

}

ostream& Select::print(ostream& _os) {
	string output = "SELECT [ SCHEMA : {";

	for (auto i = 0; i < schema.GetNumAtts(); i++) {
		if (i == schema.GetNumAtts() - 1) {
			output += " " + schema.GetAtts()[i].name;
		}
		output += " " + schema.GetAtts()[i].name + ",";
	}

	output += " }; predicate : { ";
	return _os << output << predicate << "} ]";
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput, int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	numAttsInput = _numAttsInput;
	numAttsOutput = _numAttsOutput;
	keepMe = _keepMe;
	producer = _producer;
}

Project::~Project() {

}

ostream& Project::print(ostream& _os) {
	string output = "PROJECT [ schemaIN : {";

	for (auto i = 0; i < numAttsInput; i++) {
		if (i == schemaIn.GetNumAtts() - 1) {
			output += " " + schemaIn.GetAtts()[i].name;
		}
		output += " " + schemaIn.GetAtts()[i].name + ",";
	}

	output += "}; schemaOUT : {"; 

	for (auto i = 0; i < numAttsOutput; i++) {
		if (i == schemaOut.GetNumAtts() - 1) {
			output += " " + schemaOut.GetAtts()[i].name;
		}
		output += " " + schemaOut.GetAtts()[i].name + ",";
	}

	output += " } ]";
	return _os << output;
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut, CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {
	schemaLeft = _schemaLeft;
	schemaRight = _schemaRight;
	schemaOut = _schemaOut;
	predicate = _predicate;
	left = _left;
	right = _right;
}

Join::~Join() {

}

ostream& Join::print(ostream& _os) {
	string output = "JOIN [ schemaL : {";

	for (auto i = 0; i < schemaLeft.GetNumAtts(); i++) {
		if (i == schemaLeft.GetNumAtts() - 1) {
			output += " " + schemaLeft.GetAtts()[i].name;
		}
		output += " " + schemaLeft.GetAtts()[i].name + "u";
	}

	output += "}; sR : {" + schemaRight.GetAtts()[0].name + "}; sOUT : {";

	for (auto i = 0; i < schemaOut.GetNumAtts(); i++) {
		if (i == schemaOut.GetNumAtts() - 1) {
			output += " " + schemaOut.GetAtts()[i].name;
		}
		output += " " + schemaOut.GetAtts()[i].name + "u";
	}

	output += " } ]";
	return _os << output;
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {
	schema = _schema;
	producer = _producer;
}

DuplicateRemoval::~DuplicateRemoval() {

}

ostream& DuplicateRemoval::print(ostream& _os) {
	string output = "DISTINCT [ schemaIN : {";

	for (auto i = 0; i < schema.GetNumAtts(); i++) {
		if (i == schema.GetNumAtts() - 1) {
			output += " " + schema.GetAtts()[i].name;
		}
		output += " " + schema.GetAtts()[i].name + ",";
	}

	output += " } ]";
	return _os << output;
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute, RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	compute = _compute;
	producer = _producer;
}

Sum::~Sum() {

}

// SUM [ F : l_extendedprice * l_discount; sIN : { l_orderkey, … }; sOUT : { SUM : FLOAT } ] 
ostream& Sum::print(ostream& _os) {
	string output = "; sIN : {";

	for (auto i = 0; i < schemaIn.GetNumAtts(); i++) {
		if (i == schemaIn.GetNumAtts() - 1) {
			output += " " + schemaIn.GetAtts()[i].name;
		}
		output += " " + schemaIn.GetAtts()[i].name + ",";
	}

	for (auto i = 0; i < schemaIn.GetNumAtts(); i++) {
		if (i == schemaIn.GetNumAtts() - 1) {
			output += "}; sOUT : {" + to_string(schemaOut.GetAtts()[i].type);
		}
		output += "}; sOUT : {" + to_string(schemaOut.GetAtts()[i].type) + " } ]";
	}
	
	return _os << "SUM [ F : {" << output;
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts, Function& _compute,	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	groupingAtts = _groupingAtts;
	compute = _compute;
	producer = _producer;
}

GroupBy::~GroupBy() {

}

ostream& GroupBy::print(ostream& _os) {
	string output = "GROUP BY [ schemaIN : {";
	// for (auto i = 0; i < numAttsInput; i++) {
	// 	output += " " + schemaIn.GetAtts()[i].name + ",";
	// }
	return _os << output;
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {
	schema = _schema;
	outFile = _outFile;
	producer = _producer;
	myFile.open (&outFile[0]);
}

WriteOut::~WriteOut() {

}

ostream& WriteOut::print(ostream& _os) {
	return _os << "OUTPUT:\n{\n\t" << *producer <<"\n}\n";
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	return _os << "QUERY EXECUTION TREE";
}
