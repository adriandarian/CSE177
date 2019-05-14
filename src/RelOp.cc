#include <iostream>
#include <sstream>
#include <cstring>
#include <list>

#include "RelOp.h"
#include "TwoWayList.h"

using namespace std;

ostream &operator<<(ostream &_os, RelationalOp &_op)
{
	return _op.print(_os);
}

Scan::Scan(Schema &_schema, DBFile &_file)
{
	schema = _schema;
	file = _file;
}
bool Scan::GetNext(Record &_record)
{
	//	cout << "---- SCAN ------" << endl;

	if (file.GetNext(_record) == 0)
	{
		//if there is a record, return
		return true;
	}
	else
	{
		//no records
		return false;
	}
}
Scan::~Scan()
{
	//printf("Deconstructor Scan\n");
}

ostream &Scan::print(ostream &_os)
{
	return _os << "SCAN[" << file.GetFile() << "]" << endl;
}

Select::Select(Schema &_schema, CNF &_predicate, Record &_constants,
							 RelationalOp *_producer)
{
	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;
}

Select::~Select()
{
	//printf("Deconstructor Select\n");
}

bool Select::GetNext(Record &_record)
{
	while (producer->GetNext(_record))
	{
		if (predicate.Run(_record, constants))
		{
			return true;
		}
	}
	return false;
}

ostream &Select::print(ostream &_os)
{
	_os << "SELECT[ Schema:{";
	vector<Attribute> a = schema.GetAtts();
	int j = 0;
	for (int i = 0; i < a.size(); i++)
	{
		_os << a[i].name;
		if (i != a.size() - 1)
		{
			_os << ", ";
		}
	}
	_os << "}; Predicate (";

	for (int i = 0; i < predicate.numAnds; i++)
	{
		Comparison c = predicate.andList[j];
		if (c.operand1 != Literal)
		{
			_os << a[c.whichAtt1].name;
		}
		else
		{
			int pointer = ((int *)constants.GetBits())[i + 1];
			if (c.attType == Integer)
			{
				int *myInt = (int *)&(constants.GetBits()[pointer]);
				_os << *myInt;
			}
			// then is a double
			else if (c.attType == Float)
			{
				double *myDouble = (double *)&(constants.GetBits()[pointer]);
				_os << *myDouble;
			}
			// then is a character string
			else if (c.attType == String)
			{
				char *myString = (char *)&(constants.GetBits()[pointer]);
				_os << myString;
			}
		}
		if (c.op == Equals)
		{
			_os << " = ";
		}
		else if (c.op == GreaterThan)
		{
			_os << " > ";
		}
		else if (c.op == LessThan)
		{
			_os << " < ";
		}

		if (c.operand2 != Literal)
		{
			_os << a[c.whichAtt2].name;
		}
		else
		{
			int pointer = ((int *)constants.GetBits())[i + 1];
			if (c.attType == Integer)
			{
				int *myInt = (int *)&(constants.GetBits()[pointer]);
				_os << *myInt;
			}
			// then is a double
			else if (c.attType == Float)
			{
				double *myDouble = (double *)&(constants.GetBits()[pointer]);
				_os << *myDouble;
				//_os << "issafloat";
			}
			// then is a character string
			else if (c.attType == String)
			{
				char *myString = (char *)&(constants.GetBits()[pointer]);
				_os << myString;
				//_os << "issastring";
			}
		}
		j++;

		if (i < predicate.numAnds - 1)
		{
			_os << " AND ";
		}
	}

	return _os << ")] \n\t\t\t--" << *producer << endl;
}

Project::Project(Schema &_schemaIn, Schema &_schemaOut, int _numAttsInput,
								 int _numAttsOutput, int *_keepMe, RelationalOp *_producer)
{
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	numAttsInput = _numAttsInput;
	numAttsOutput = _numAttsOutput;
	keepMe = _keepMe;
	producer = _producer;
}

Project::~Project()
{
	//printf("Deconstructor Project\n");
}

bool Project::GetNext(Record &_record)
{

	// Call for projector operator
	bool ret = producer->GetNext(_record);
	if (ret)
	{
		_record.Project(keepMe, numAttsOutput, numAttsInput);
		return true;
	}
	else
	{
		return false;
	}
}

ostream &Project::print(ostream &_os)
{
	_os << "PROJECT[ schemaIn: {";
	for (int i = 0; i < schemaIn.GetAtts().size(); i++)
	{
		_os << schemaIn.GetAtts()[i].name;
		if (i != schemaIn.GetAtts().size() - 1)
		{
			_os << ", ";
		}
	}
	_os << "},schemaOut: {";
	for (int i = 0; i < schemaOut.GetAtts().size(); i++)
	{
		_os << schemaOut.GetAtts()[i].name;
		if (i != schemaOut.GetAtts().size() - 1)
		{
			_os << ", ";
		}
	}
	_os << "}]" << endl;
	return _os << "\t\n\t--" << *producer;
}

Join::Join(Schema &_schemaLeft, Schema &_schemaRight, Schema &_schemaOut,
					 CNF &_predicate, RelationalOp *_left, RelationalOp *_right)
{
	schemaLeft = _schemaLeft;
	schemaRight = _schemaRight;
	schemaOut = _schemaOut;
	predicate = _predicate;
	left = _left;
	right = _right;
}

Join::~Join()
{
	//printf("Deconstructor Join\n");
}

bool Join::NestedLoopJoin(Record &_record)
{
	if (leftNode)
	{
		while (left->GetNext(record))
		{
			// record.print(cout, schemaLeft);
			// cout << endl;
			NLJ.Insert(record);
		}
		leftNode = false;
	}

	while (right->GetNext(record))
	{
		NLJ.MoveToStart();
		// record.print(cout, schemaRight);
		// cout << endl;
		while (!NLJ.AtEnd())
		{
			currentRecord = NLJ.Current();
			if (predicate.Run(currentRecord, record))
			{
				_record.AppendRecords(currentRecord, record, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
				// _record.print(cout, schemaOut);
				// cout << endl;
				return true;
			}
			NLJ.Advance();
		}
	}

	return false;
}

bool Join::HashJoin(Record &_record)
{
	if (leftNode)
	{
		while (left->GetNext(record))
		{
			HJ.Insert(record);
		}
		leftNode = false;
	}

	while (right->GetNext(record))
	{
		HJ.MoveToStart();
		while (!HJ.AtEnd())
		{
			currentRecord = HJ.Current();
			if (predicate.Run(currentRecord, record))
			{
				_record.AppendRecords(currentRecord, record, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
				return true;
			}
			HJ.Advance();
		}
	}

	return false;
}

bool Join::SymmetricHashJoin(Record &_record)
{
	if (leftNode)
	{
		while (left->GetNext(record))
		{
			SHJ.Insert(record);
		}
		leftNode = false;
	}

	while (right->GetNext(record))
	{
		SHJ.MoveToStart();
		while (!SHJ.AtEnd())
		{
			currentRecord = SHJ.Current();
			if (predicate.Run(currentRecord, record))
			{
				_record.AppendRecords(currentRecord, record, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
				return true;
			}
			SHJ.Advance();
		}
	}

	return false;
}

bool Join::GetNext(Record &_record)
{
	NestedLoopJoin(_record);
}

ostream &Join::print(ostream &_os)
{
	_os << "JOIN[ schemaLeft: {";
	for (int i = 0; i < schemaLeft.GetAtts().size(); i++)
	{
		_os << schemaLeft.GetAtts()[i].name;
		if (i != schemaLeft.GetAtts().size() - 1)
		{
			_os << ", ";
		}
	}
	_os << "},\n";
	for (int i = 0; i < push + 1; i++)
	{
		_os << "\t";
	}
	_os << "schemaRight: {";
	for (int i = 0; i < schemaRight.GetAtts().size(); i++)
	{
		_os << schemaRight.GetAtts()[i].name;
		if (i != schemaRight.GetAtts().size() - 1)
		{
			_os << ", ";
		}
	}
	_os << "},\n";
	for (int i = 0; i < push + 1; i++)
	{
		_os << "\t";
	}
	_os << "schemaOut: {";
	for (int i = 0; i < schemaOut.GetAtts().size(); i++)
	{
		_os << schemaOut.GetAtts()[i].name;
		if (i != schemaOut.GetAtts().size() - 1)
		{
			_os << ", ";
		}
	}
	_os << "}]\tNumber of Tuples: " << size << endl;
	for (int i = 0; i < push + 1; i++)
	{
		_os << "\t";
	}
	_os << "--" << *right;
	for (int i = 0; i < push + 1; i++)
	{
		_os << "\t";
	}
	return _os << "--" << *left;
}

DuplicateRemoval::DuplicateRemoval(Schema &_schema, RelationalOp *_producer)
{
	schema = _schema;
	producer = _producer;
}

DuplicateRemoval::~DuplicateRemoval()
{
	//printf("Deconstructor DuplicateRemoval\n");
}

bool DuplicateRemoval::GetNext(Record &_record)
{
	while (producer->GetNext(_record))
	{
		stringstream temp;
		_record.print(temp, schema);
		_record.GetBits();
		//temp << endl;
		bool exists = recordSet.find(temp.str()) != recordSet.end();
		if (!exists)
		{
			recordSet.insert(temp.str());
			return true;
		}
	}
	return false;
}

ostream &DuplicateRemoval::print(ostream &_os)
{
	_os << "DISTINCT[{";
	for (int i = 0; i < schema.GetAtts().size(); i++)
	{
		_os << schema.GetAtts()[i].name;
		if (i != schema.GetAtts().size() - 1)
		{
			_os << ", ";
		}
	}
	return _os << "}]"
						 << "\n\t\n\t--" << *producer;
}

Sum::Sum(Schema &_schemaIn, Schema &_schemaOut, Function &_compute,
				 RelationalOp *_producer)
{
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	compute = _compute;
	producer = _producer;
}

Sum::~Sum()
{
	//printf("Deconstructor Sum\n");
}

/*
	* Function: string GetType(), Type Apply(Record toMe, Int intResult, Double dblResult) 
*/

//TODO: Implement
bool Sum::GetNext(Record &_record)
{
	double runningSum = 0;
	Type typeSum;
	Record tempRec;
	bool infiniteLoop = false;
	char *bits = new char[1];
	while (producer->GetNext(tempRec))
	{
		//Don't forget to set to 0
		int tempInt = 0;
		double tempDouble = 0;
		typeSum = compute.Apply(tempRec, tempInt, tempDouble);
		runningSum += tempInt + tempDouble;
		infiniteLoop = true;
	}
	//TODO: Change - Unsure
	if (infiniteLoop)
	{
		char *recordResult;
			*((double *)bits) = runningSum;
			recordResult = new char[2 * sizeof(int) + sizeof(double)];
			((int *)recordResult)[0] = sizeof(int) + sizeof(int) + sizeof(double);
			((int *)recordResult)[1] = sizeof(int) + sizeof(int);
			memcpy(recordResult + 2 * sizeof(int), bits, sizeof(double));
		
		_record.Consume(recordResult);
		//infiniteLoop = false;
		return true;
	}
	else
	{
		return false;
	}
}

ostream &Sum::print(ostream &_os)
{
	_os << "SUM:[ schemaIn: {";
	for (int i = 0; i < schemaIn.GetAtts().size(); i++)
	{
		_os << schemaIn.GetAtts()[i].name;
		if (i != schemaIn.GetAtts().size() - 1)
		{
			_os << ", ";
		}
	}
	_os << "},schemaOut: {";
	for (int i = 0; i < schemaOut.GetAtts().size(); i++)
	{
		_os << schemaOut.GetAtts()[i].name;
		if (i != schemaOut.GetAtts().size() - 1)
		{
			_os << ", ";
		}
	}
	return _os << "}]"
						 << "\n\t\n\t--" << *producer;
}

GroupBy::GroupBy(Schema &_schemaIn, Schema &_schemaOut, OrderMaker &_groupingAtts,
								 Function &_compute, RelationalOp *_producer)
{
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	groupingAtts = _groupingAtts;
	compute = _compute;
	producer = _producer;
	isFirst = true;
}

GroupBy::GroupBy(Schema &_schemaIn, Schema &_schemaOut, OrderMaker &_groupingAtts,
								 Function &_compute, FuncOperator *_parseTree, RelationalOp *_producer)
{
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	//	_schemaOut.Swap(schemaIn);
	//	schemaOut.Swap(schemaIn);
	groupingAtts = _groupingAtts;
	compute = _compute;
	producer = _producer;
	parseTree = _parseTree;
	Function copyOfFunction(_compute);
	Type retType = copyOfFunction.RecursivelyBuild(parseTree, _schemaIn);
	vector<string> attrsType;

}

GroupBy::~GroupBy()
{
	//printf("Deconstructor GroupBy\n");
}

//WTF Is this shit
//map with relational op usage
bool GroupBy::GetNext(Record& _record) {
	//_record.Project(groupingAtts.whichAtts,groupingAtts.numAtts,schemaIn.GetNumAtts());

	//int i = 1;
	int tempInt = 0;
	double tempDouble = 0;
	double runningSum = 0;
	Group insertGroup;
	//double insert = 0;


	if(first == true) {
		first = false;
		Record tempRec;
		Record *newRec = new Record();

	while(true) {
		newRec->Project(groupingAtts.whichAtts,groupingAtts.numAtts,schemaIn.GetNumAtts());
	
	//while(true) {
	//cout << "Group By Enter While" << endl;
		tempInt = 0;
		tempDouble = 0.0;

		string groupByString;
		tempSchema = schemaOut;
		double insert = 0;

			tempInt = 0;
			tempDouble = 0;
			compute.Apply(*newRec, tempInt, tempDouble);
			insert = tempInt + tempDouble;



		newRec->Project(groupingAtts.whichAtts, groupingAtts.numAtts, schemaIn.GetNumAtts());


//-	************************************************************
	
		//Get Name		
		if(groupingAtts.whichTypes[0] == Type::String) {
			char* groupByChar = newRec->GetColumn(groupingAtts.whichAtts[0]);
			groupByString = string(groupByChar);
			it = mapGroupBy.find(groupByString);
		}
		else if(groupingAtts.whichTypes[0] == Type::Integer) {
			int groupByInt = *((int *) newRec->GetColumn(groupingAtts.whichAtts[0]));
			groupByString = to_string(groupByInt);
			it = mapGroupBy.find(groupByString);
		}
		else {
			float groupByFloat = *((double *) newRec->GetColumn(groupingAtts.whichAtts[0]));
			groupByString = to_string(groupByFloat);
			it = mapGroupBy.find(groupByString);
		}
		
//-*******************************************************************************	
//-*******************************************************************************		

		if(it != mapGroupBy.end()){
			//cout << "Does Exist If" << endl;
			it->second.runningSum += insert;
			it->second.rec = *newRec;

		}
		else { // Does not Exist - Insert
			insertGroup.rec = *newRec;
			insertGroup.runningSum = insert;
			mapGroupBy[groupByString] = insertGroup;
		}
//	} 

		it = mapGroupBy.begin();



	//Print statement
//	cout << "Print Statement" << end;


//  =============================================================================
//  =============================================================================
//  Put all in records for returning
//  =============================================================================
//  ============================================================================= * 
//
	

		// return new record and advance iterator
		//_record = newRec;
		//it++;

		return true;
	} // Record Joining For Loop

			//if(it == mapGroupBy.end()) {
			//return false;
		//}
	//}  -- From if statement??
	

		if(!mapGroupBy.empty()) {
			for(auto iter = mapGroupBy.begin(); iter != mapGroupBy.end(); ++iter) {
				if(iter->second.rec.GetSize() != 0) {
					// Record * output = iter->second.rec;
					_record.Swap(iter->second.rec);
					//it1->second->pop_front();
					return true;
				}
			}
		}

	}
}

RelationalOp *GroupBy::GetProducer()
{
	return producer;
}

ostream &GroupBy::print(ostream &_os)
{
	_os << "GROUP BY[ schemaIn: {";
	for (int i = 0; i < schemaIn.GetAtts().size(); i++)
	{
		_os << schemaIn.GetAtts()[i].name;
		if (i != schemaIn.GetAtts().size() - 1)
		{
			_os << ", ";
		}
	}
	_os << "},schemaOut: {";
	for (int i = 0; i < schemaOut.GetAtts().size(); i++)
	{
		_os << schemaOut.GetAtts()[i].name;
		if (i != schemaOut.GetAtts().size() - 1)
		{
			_os << ", ";
		}
	}
	_os << "}]" << endl;
	return _os << "\t\n\t--" << *producer;
}

Create::Create(Schema &_schema, CNF &_predicate, Record &_constants,
							 RelationalOp *_producer)
{
	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;
}

Create::~Create() {

}

bool Create::GetNext(Record& _record) {

}

ostream &Create::print(ostream &_os) {
		_os << "CREATE [{";
	for (int i = 0; i < schema.GetAtts().size(); i++)
	{
		_os << schema.GetAtts()[i].name;
		if (i != schema.GetAtts().size() - 1)
		{
			_os << ", ";
		}
	}
	_os << "}]" << endl;
	return _os;
}

LoadData::LoadData(Schema &_schema, string &_inFile, RelationalOp *_producer)
{
	schema = _schema;
	inFile = _inFile;
	producer = _producer;
}

LoadData::~LoadData() {

}

bool LoadData::GetNext(Record& _record) {

}

ostream &LoadData::print(ostream &_os) {
		_os << "LOAD DATA [{";
	for (int i = 0; i < schema.GetAtts().size(); i++)
	{
		_os << schema.GetAtts()[i].name;
		if (i != schema.GetAtts().size() - 1)
		{
			_os << ", ";
		}
	}
	_os << "}]" << endl;
	return _os;
}

WriteOut::WriteOut(Schema &_schema, string &_outFile, RelationalOp *_producer)
{
	schema = _schema;
	outFile = _outFile;
	producer = _producer;
	outFile = _outFile;
	//Open the file stream
	out.open(outFile.c_str());
	//cout << "---- enter ------" << endl;
}

WriteOut::~WriteOut()
{
	//printf("Deconstructor WriteOut\n");
	//if filestream is open, close it
	if (out.is_open())
	{
		out.close();
	}
}

bool WriteOut::GetNext(Record &_record)
{
	//cout << "---- IN WRITE OUT ------" << endl;
	bool ret = producer->GetNext(_record);
	if (ret)
	{
		//	cout << "---- IF STATEMENT ------" << endl;

		//Lecture notes line below.
		//outFile << _record.print(schema);
		_record.print(out, schema);
		out << endl;
		_record.print(cout, schema);
		cout << endl;
		return true;
	}
	else
	{
		out.close();
		return false;
	}
}

ostream &WriteOut::print(ostream &_os)
{
	_os << "WRITE OUT [{";
	for (int i = 0; i < schema.GetAtts().size(); i++)
	{
		_os << schema.GetAtts()[i].name;
		if (i != schema.GetAtts().size() - 1)
		{
			_os << ", ";
		}
	}
	_os << "}]" << endl;
	return _os << "\t--" << *producer;
}

void QueryExecutionTree::ExecuteQuery()
{
	Record rec;
	// cout << "---- EXECUTE QUERY ------" << endl;
	while (root->GetNext(rec))
	{
		//This is enough
	}
}

ostream &operator<<(ostream &_os, QueryExecutionTree &_op)
{
	_os << "QUERY EXECUTION TREE " << endl;
	return _os << "--" << *_op.root;
}