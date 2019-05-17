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
	if (file.GetNext(_record) == 0)
	{
		// if there is a record, return
		return true;
	}
	else
	{
		// no records
		return false;
	}
}
Scan::~Scan()
{
}

ostream &Scan::print(ostream &_os)
{
	return _os << "SCAN[" << file.GetFile() << "]" << endl;
}

Select::Select(Schema &_schema, CNF &_predicate, Record &_constants, RelationalOp *_producer)
{
	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;
}

Select::~Select()
{
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

Project::Project(Schema &_schemaIn, Schema &_schemaOut, int _numAttsInput, int _numAttsOutput, int *_keepMe, RelationalOp *_producer)
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
}

bool Join::NestedLoopJoin(Record &_record)
{
	if (leftNode)
	{
		while (left->GetNext(record))
		{
			NLJ.Insert(record);
		}

		NLJ.MoveToStart();
		while (!NLJ.AtEnd()) {
			NLJ.Current().print(cout, schemaOut);
			NLJ.Advance();
		}

		leftNode = false;
	}

	while (right->GetNext(record))
	{
		NLJ.MoveToStart();
		while (!NLJ.AtEnd())
		{
			currentRecord = NLJ.Current();
			if (predicate.Run(currentRecord, record))
			{
				_record.AppendRecords(currentRecord, record, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
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
	cout << "\nrunning Join\n";
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

	sent = 0;
}

Sum::~Sum()
{
}

bool Sum::GetNext(Record &_record)
{
	if (sent)
		return false;

	int intSum = 0;
	double doubleSum = 0;

	while (producer->GetNext(_record))
	{
		int intResult = 0;
		double doubleResult = 0;
		Type t = compute.Apply(_record, intResult, doubleResult);

		if (t == Integer)
			intSum += intResult;
		if (t == Float)
			doubleSum += doubleResult;
	}

	double val = doubleSum + (double)intSum;
	char *recSpace = new char[PAGE_SIZE];
	int currentPosInRec = sizeof(int) * (2);
	((int *)recSpace)[1] = currentPosInRec;
	*((double *)&(recSpace[currentPosInRec])) = val;
	currentPosInRec += sizeof(double);
	((int *)recSpace)[0] = currentPosInRec;
	Record sumRec;

	sumRec.CopyBits(recSpace, currentPosInRec);

	delete[] recSpace;

	_record = sumRec;
	sent = 1;

	return true;
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

GroupBy::GroupBy(Schema &_schemaIn, Schema &_schemaOut, OrderMaker &_groupingAtts, Function &_compute, RelationalOp *_producer) : groupingAtts(_groupingAtts)
{
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	// groupingAtts = _groupingAtts;
	compute = _compute;
	producer = _producer;

	phase = 0;

	isFirst = true;
}

GroupBy::GroupBy(Schema &_schemaIn, Schema &_schemaOut, OrderMaker &_groupingAtts, Function &_compute, FuncOperator *_parseTree, RelationalOp *_producer)
{
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
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
}

bool GroupBy::GetNext(Record &_record)
{
	vector<int> attsToKeep, attsToKeep1;
	for (int i = 1; i < schemaOut.GetNumAtts(); i++)
		attsToKeep.push_back(i);

	copy = schemaOut;
	copy.Project(attsToKeep);

	attsToKeep1.push_back(0);
	sum = schemaOut;
	sum.Project(attsToKeep1);

	if (phase == 0)
	{
		while (producer->GetNext(_record))
		{
			stringstream s;
			int iResult = 0;
			double dResult = 0;
			compute.Apply(_record, iResult, dResult);
			double val = dResult + (double)iResult;

			_record.Project(&groupingAtts.whichAtts[0], groupingAtts.numAtts, copy.GetNumAtts());
			_record.print(s, copy);
			auto it = set.find(s.str());

			if (it != set.end())
				set[s.str()] += val;
			else
			{
				set[s.str()] = val;
				recMap[s.str()] = _record;
			}
		}
		phase = 1;
	}

	if (phase == 1)
	{
		if (set.empty())
			return false;

		Record temp = recMap.begin()->second;
		string strr = set.begin()->first;

		char *recSpace = new char[PAGE_SIZE];
		int currentPosInRec = sizeof(int) * (2);
		((int *)recSpace)[1] = currentPosInRec;
		*((double *)&(recSpace[currentPosInRec])) = set.begin()->second;
		currentPosInRec += sizeof(double);
		((int *)recSpace)[0] = currentPosInRec;
		Record sumRec;
		sumRec.CopyBits(recSpace, currentPosInRec);
		delete[] recSpace;

		Record newRec;
		newRec.AppendRecords(sumRec, temp, 1, schemaOut.GetNumAtts() - 1);
		recMap.erase(strr);
		set.erase(strr);
		_record = newRec;
		return true;
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

Create::Create(Schema &_schema, CNF &_predicate, Record &_constants, RelationalOp *_producer)
{
	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;
}

Create::~Create()
{
}

bool Create::GetNext(Record &_record)
{
}

ostream &Create::print(ostream &_os)
{
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

LoadData::~LoadData()
{
}

bool LoadData::GetNext(Record &_record)
{
}

ostream &LoadData::print(ostream &_os)
{
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

	// Open the file stream
	out.open(outFile.c_str());
}

WriteOut::~WriteOut()
{
	// if filestream is open, close it
	if (out.is_open())
	{
		out.close();
	}
}

bool WriteOut::GetNext(Record &_record)
{
	bool ret = producer->GetNext(_record);
	if (ret)
	{
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
	unsigned long recs = 0;
	cout << "---- EXECUTE QUERY ------\n"
			 << endl;
	while (root->GetNext(rec))
	{
		recs++;
	}
	cout << "\n---------Records in output file : " << recs << "---------\n";
}

ostream &operator<<(ostream &_os, QueryExecutionTree &_op)
{
	_os << "QUERY EXECUTION TREE " << endl;
	return _os << "--" << *_op.root;
}