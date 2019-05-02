#include <iostream>
#include <cstring>
#include <sstream>
#include "RelOp.h"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {
	schema = _schema;
	file = _file;
}

Scan::~Scan() {

}

bool Scan::GetNext(Record& _record) {
	if (file.GetNext(_record) == 0) {
		return true;
	}
	return false;
}

ostream& Scan::print(ostream& _os) {
	//file.resetCurrentPage();
	return _os << "\t\tSCAN [" << tableName << "]";
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {
	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;
}

Select::~Select() {

}

//Modified version of pseudocode from lecture
bool Select::GetNext(Record& _record) {
	bool ret;
	while(ret = producer->GetNext(_record)) {
		if(ret) {
			if(predicate.Run(_record, constants))
				return true;
		}
		else {
			return false;
		}
	}
	return false;
}

ostream& Select::print(ostream& _os) {
	_os << "SELECT [schema: {";
	for(int i = 0; i < predicate.numAnds; i++) {
	 	if(i > 0) {
	 		_os << " AND ";
	 	}

	 	Comparison compare = predicate.andList[i];
	 	vector<Attribute> atts = schema.GetAtts();
		//prints 1st attribute in predicate
		if (compare.operand1 == Literal) {
			int pointer = ((int *) constants.GetBits())[compare.whichAtt1 + 1];
	 		if (compare.attType == Integer) {
	 			int* myInt = (int *) &(constants.GetBits()[pointer]);
	 			_os << *myInt;
	 		} else if (compare.attType == Float) {
	 			double* myDouble = (double *) &(constants.GetBits()[pointer]);
	 			_os << *myDouble;
	 		} else if (compare.attType == String) {
	 			char* myString = (char *) &(constants.GetBits()[pointer]);
	 			_os << "\'" << myString << "\'";
	 		}
		} else {
			_os << atts[compare.whichAtt1].name;
		}

		// print bool operator
		switch (compare.op) {
			case Equals:
			{
				_os << " = ";
			}
				break;
			case LessThan:
			{
				_os << " < ";
			}
				break;
			case GreaterThan:
			{
				_os << " > ";
			}
				break;
		}
		

		//prints 2nd attribute in predicate
		if (compare.operand2 == Literal) {
			int pointer = ((int *) constants.GetBits())[compare.whichAtt2 + 1];
			if (compare.attType == Integer) {
	 			int* myInt = (int *) &(constants.GetBits()[pointer]);
	 			_os << *myInt;
	 		} else if (compare.attType == Float) {
	 			double* myDouble = (double *) &(constants.GetBits()[pointer]);
	 			_os << *myDouble;
	 		} else if (compare.attType == String) {
	 			char* myString = (char *) &(constants.GetBits()[pointer]);
	 			_os << "\'" << myString << "\'";
	 		}
		} else {
			_os << atts[compare.whichAtt2].name;
		}
	 }
	 _os << "}]" << endl;
	 
	 _os << *producer;
	 return _os;
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	// numAttsInput is current number of attribute present in record
	numAttsInput = _numAttsInput;
	// numAttsOutput = number of atrribute to keep
	numAttsOutput = _numAttsOutput;
	keepMe = _keepMe;
	producer = _producer;
}

Project::~Project() {

}

//Lecture Notes
bool Project::GetNext(Record& _record) {
	if (producer->GetNext(_record)) {
		_record.Project( keepMe, numAttsOutput, numAttsInput);
		return true;
	}
	return false;
}

ostream& Project::print(ostream& _os) {
	_os << "PROJECT [schemaIN:{";
	vector<Attribute>attsIN = schemaIn.GetAtts();
	for(auto it = attsIN.begin(); it != attsIN.end(); it++) {
	 	if(it != attsIN.begin())
	 		_os << ", ";
	 	_os << it->name;
	 }
	 _os << "},"<< endl;
	 _os << "\tschemaOUT:{";
	vector<Attribute> atts = schemaOut.GetAtts();
	 for(auto it = atts.begin(); it != atts.end(); it++) {
	 	if(it != atts.begin())
	 		_os << ", ";
	 	_os << it->name;
	 }
	 _os << "}]" << endl << endl;
	 _os << *producer;
	 return _os;
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {
	schemaLeft = _schemaLeft;
	schemaRight = _schemaRight;
	schemaOut = _schemaOut;
	predicate = _predicate;
	left = _left;
	right = _right;
}

void Open() {
	R.Open();
	S.Open();

	s = S.GetNext() // s = current tuple of S
}

void Close() {
	R.Close();
	S.Close();
}

// GetNext(): output the next (just 1 !!!) tuple in the join R ⋈ S
void NestedLoopJoin() {
	// Note: s already has the current (next ?) tuple of relation S
	while (true) {
		// Loop exits when:
		// 	1. we found a tuple ∈ R that joins with s
		// 	2. R ⋈ S is done (returns NotFound)

		// Get the next tuple r ∈ R to Join
		r = R.GetNext(); // Get next tuple in R to perform Join

		if (r == NotFound) {
			// Tuple s has joined with every tuple ∈ R
			// Use next tuple ∈ S in the Join
			s = S.GetNext();

			if (s == NotFound) {
				// We have processed the last tuple ∈ S
				return NotFound; // Done !!!
			}

			// We have a new tuple s ∈ S
			// Restart R from the beginning
			R.Close(); // Close first
			R.Open(); // Reset R to the beginning
			r = R.GetNext(); // Get current tuple in R
		}

		// When we reach here, we have:
		// 	s = current tuple in S for the Join operation
		// 	r = current tuple in R for the Join operation

		if (r(Y) == s(Y)) {
			return (r, s); // Return next tuple of Join
		}

		// Repeat and try the next tuple in R
	}
}

bool Join::GetNext(Record& _record) {

}

Join::~Join() {

}

ostream& Join::print(ostream& _os) {
	_os << "JOIN [schemaLeft: {";
	vector<Attribute>attsLeft = schemaLeft.GetAtts();
	for(auto it = attsLeft.begin(); it != attsLeft.end(); it++) {
	 	if(it != attsLeft.begin())
	 		_os << ", ";
	 	_os << it->name;
	}
	_os << "}, " << endl;

	_os << "\tschemaRight: { ";
	vector<Attribute>attsRight = schemaRight.GetAtts();
	for(auto it = attsRight.begin(); it != attsRight.end(); it++) {
	 	if(it != attsRight.begin())
	 		_os << ", ";
	 	_os << it->name;
	 }
	 _os << "}, "<< endl;

	_os << "\tschemaOut: {";
	vector<Attribute>attsOut = schemaOut.GetAtts();
	for(auto it = attsOut.begin(); it != attsOut.end(); it++) {
	 	if(it != attsOut.begin())
	 		_os << ", ";
	 	_os << it->name;
	}
		_os << "}]" << endl << endl;

		_os << *right << endl;
	
	 	_os << *left << endl;
	return _os;
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {
	schema = _schema;
	producer = _producer;
}

DuplicateRemoval::~DuplicateRemoval() {

}


bool DuplicateRemoval::GetNext(Record& _record) {
	while(producer->GetNext(_record)) {
		stringstream temp;
		_record.print(temp, schema);
		//temp << endl;
		bool exists = recordSet.find(temp.str()) != recordSet.end();
		if(!exists) {
			recordSet.insert(temp.str());
			return true;
		}
		
	}
	return false;
}

ostream& DuplicateRemoval::print(ostream& _os) {
	_os << "DISTINCT [{";

	vector<Attribute> atts = schema.GetAtts();
	for(auto it = atts.begin(); it != atts.end(); it++) {
	 	if(it != atts.begin())
	 		_os << ", ";
	 	_os << it->name;
	 }
	 _os << "}]" << endl << endl;
	 _os << *producer;
	 return _os;
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	compute = _compute;
	producer = _producer;
}

Sum::~Sum() {

}

/*
	* Function: string GetType(), Type Apply(Record toMe, Int intResult, Double dblResult) 
*/
bool Sum::GetNext(Record& _record) {

	while(producer->GetNext(tempRec)) {
		//Don't forget to set to 0
		int tempInt = 0;
		double tempDouble = 0;
		typeSum = compute.Apply(tempRec, tempInt, tempDouble);
		runningSum += tempInt + tempDouble;
		infiniteLoop = true;
	}
	if(infiniteLoop) {
		char* recordResult;
		if(typeSum == Float) {
			*((double *) bits) = runningSum;
			recordResult = new char[2*sizeof(int) + sizeof(double)];
			((int*) recordResult)[0] = sizeof(int) + sizeof(int) + sizeof(double);
			((int*) recordResult)[1] = sizeof(int) + sizeof(int);
			memcpy(recordResult+2*sizeof(int), bits, sizeof(double));
		}
		_record.Consume(recordResult);
		return true;
	}
	else {
		return false;
	}
	
}

ostream& Sum::print(ostream& _os) {
	_os << "SUM [";
	 vector<Attribute> atts = schemaOut.GetAtts();
	_os << expression;
	_os << "]" << endl;

	_os << "\tschemaIn{";
	vector<Attribute>attsIN = schemaIn.GetAtts();
	for(auto it = attsIN.begin(); it != attsIN.end(); it++) {
	 	if(it != attsIN.begin())
	 		_os << ", ";
	 	_os << it->name;
	 }
	 _os << "},"<< endl;

	 _os << "\tschemaOut{";
	 for(auto it = atts.begin(); it != atts.end(); it++) {
	 	if(it != atts.begin())
	 		_os << ", ";
	 	_os << it->name;
	 }
	 _os << "}]"<< endl << endl;
	 _os << *producer;
	 return _os;
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	groupingAtts = _groupingAtts;
	compute = _compute;
	producer = _producer;
}

GroupBy::~GroupBy() {

}

bool GroupBy::GetNext(Record& _record) {
	_record.Project(groupingAtts.whichAtts,groupingAtts.numAtts,schemaIn.GetNumAtts());

	atts = schemaOut.GetAtts();
	for(int i = 1; i < schemaOut.GetNumAtts();i++){
		attNames.push_back(atts[i].name);
	}
	int i = 1;
	int tempInt = 0;
	double tempDouble = 0;
	double runningSum = 0;
	double insert = 0;

	while(producer->GetNext(_record)){
	//cout << "Group By Enter While" << endl;
		compute.Apply(_record, tempInt, tempDouble);
		//If it already exists
		if((it = mapGroupBy.find(atts[i].name)) != mapGroupBy.end()){
			//cout << "Does Exist If" << endl;
			runningSum += tempInt + tempDouble;
			it->second = runningSum;
		}
		else { // Does not Exist - Insert
			//cout << "(" << name << ") Does Not Exist Else" << endl;
			if (atts[i].type == Float) {	
				insert = tempDouble;
				mapGroupBy.insert(make_pair(atts[i].name, insert));
			}
			else if (atts[i].type == Integer) {
				insert = tempInt;
				mapGroupBy.insert(make_pair(atts[i].name, insert));
			}			
		}
		i++;
		return true;
	}
	//cout << "Group By Exit While" << endl;

	//Print statement
	if(!mapGroupBy.empty()) {
		for(auto tempIt = mapGroupBy.begin(); tempIt != mapGroupBy.end(); ++tempIt) {
			cout << tempIt->first << " : " << tempIt->second << endl;
		}
	}

	//cout << "Group By Exit. Return False" << endl;
	return false;
}

ostream& GroupBy::print(ostream& _os) {
	_os << "GROUP BY [";

	_os << "schemaIn{";
	vector<Attribute>attsIN = schemaIn.GetAtts();
	for(auto it = attsIN.begin(); it != attsIN.end(); it++) {
	 	if(it != attsIN.begin())
	 		_os << ", ";
	 	_os << it->name;
	 }
	 _os << "},"<< endl;

	 _os << "\tschemaOut: {";
	vector<Attribute> attsOut = schemaOut.GetAtts();
	for(auto it = attsOut.begin(); it != attsOut.end(); it++) {
	 	if(it != attsOut.begin())
	 		_os << ", ";
	 	_os << it->name;
	 }
	 _os << "}]" << endl << endl;
	 _os << *producer;
	 return _os;
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {
	schema = _schema;
	outFile = _outFile;
	producer = _producer;

	myFile.open(outFile);
}

WriteOut::~WriteOut() {

}

bool WriteOut::GetNext(Record& _record) {
	bool ret = producer->GetNext(_record);
	if (ret == true) {
		_record.print(myFile, schema);
		myFile << endl;
		//For Actual Output
		_record.print(cout, schema);
		cout << endl;
		return true;
	} else {
		myFile.close();
		return false;
	}
}

ostream& WriteOut::print(ostream& _os) {
	_os << "WRITE OUT [{";
	vector<Attribute> atts = schema.GetAtts();
	for(auto it = atts.begin(); it != atts.end(); it++) {
	 	if(it != atts.begin())
	 		_os << ", ";
	 	_os << it->name;
	 }
	 _os << "}]" << endl << endl;

	 _os << *producer;
	return _os; 
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	return _os << "QUERY EXECUTION TREE:" << endl << *_op.root << endl;
}

//Lecture Notes
void QueryExecutionTree::ExecuteQuery() {
	Record rec;
	while(root->GetNext(rec)) {
		//Just Keep Swimming
	}
}