#include <iostream>
#include <sstream>
#include <cstring>
#include <list>
#include "RelOp.h"
#include "TwoWayList.h"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {
	schema = _schema;
	file = _file;
}
bool Scan::GetNext(Record& _record){
//	cout << "---- SCAN ------" << endl;

	if(file.GetNext(_record) == 0){
		//if there is a record, return
		return true;
	}else{
		//no records
 		return false;
	}
}
Scan::~Scan() {
	//printf("Deconstructor Scan\n");
}

ostream& Scan::print(ostream& _os) {
	return _os << "SCAN[" << file.GetFile()<<"]" << endl;
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {
	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;	
}

Select::~Select() {
	//printf("Deconstructor Select\n");
}

bool Select::GetNext(Record& _record){
//	cout << "---- SELECT ------" << endl;

	//While there are still records from the producer
	/*
	while(true){
		//Get the records
		bool ret = producer->GetNext(_record);
		if(!ret){
			//No more records
			return false;
		}else{
			//If there are records
			//compare the record to the constants gotten from the predicates
			ret = predicate.Run(_record,constants);
			if(ret){
				//Success, found a matching tuple
				return true;
			}
		}
	}
	*/
	// PseudoCode
	while (true) {
		if (!producer->GetNext(_record)) {
			return false;
		} else {
			if (predicate.Run(_record, constants)) {
				return true;
			}
			//TODO: Change
			// else {
			// 	return false;
			// }
		}
	}
}

ostream& Select::print(ostream& _os) {
	_os << "SELECT[ Schema:{";
	vector<Attribute> a = schema.GetAtts();
	int j = 0;
	for(int i = 0; i < a.size() ;i++){
		_os << a[i].name;
		if(i != a.size() - 1 ){
			_os << ", ";
		}
	}
	_os << "}; Predicate (";
	
	for(int i = 0; i < predicate.numAnds;i++){
		Comparison c = predicate.andList[j];
		if(c.operand1 != Literal){
			_os << a[c.whichAtt1].name;
		}else{
			int pointer = ((int *) constants.GetBits())[i + 1];
			if (c.attType == Integer) {
				int *myInt = (int *) &(constants.GetBits()[pointer]);
				_os << *myInt;
			}
			// then is a double
			else if (c.attType == Float) {
				double *myDouble = (double *) &(constants.GetBits()[pointer]);
				_os << *myDouble;
			}
			// then is a character string
			else if (c.attType == String) {
				char *myString = (char *) &(constants.GetBits()[pointer]);
				_os << myString;
			} 
		}
		if(c.op == Equals){
			_os << " = ";
		}else if(c.op == GreaterThan){
			_os << " > ";
		}else if(c.op == LessThan){
			_os << " < ";
		}
		
		if(c.operand2 != Literal){
			_os << a[c.whichAtt2].name;
		}else{
			int pointer = ((int *) constants.GetBits())[i + 1];
			if (c.attType == Integer) {
				int *myInt = (int *) &(constants.GetBits()[pointer]);
				_os << *myInt;
			}
			// then is a double
			else if (c.attType == Float) {
				double *myDouble = (double *) &(constants.GetBits()[pointer]);
				_os << *myDouble;
				//_os << "issafloat";
			}
			// then is a character string
			else if (c.attType == String) {
				char *myString = (char *) &(constants.GetBits()[pointer]);
				_os << myString;
				//_os << "issastring";
			} 
		}
		j++;

		if(i < predicate.numAnds -1){
			_os << " AND ";
		}
	}
	return _os << ")] \t--" << *producer<< endl;
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	numAttsInput = _numAttsInput;
	numAttsOutput = _numAttsOutput;
	keepMe = _keepMe;
	producer = _producer;
}

Project::~Project() {
	//printf("Deconstructor Project\n");
}

bool Project::GetNext(Record& _record)
{
//	cout << "---- PROJECT ------" << endl;
	/*
	//Get the produces record
	bool ret = producer->GetNext(_record);
	if (ret)
	{
		//Success project the schema
		_record.Project(keepMe,numAttsOutput,numAttsInput);
		return true;
	}
	else
	{
		//Nothing left to project
		//cout << "false project" << endl;
		return false;
	}
	*/
	//cout << "---- PROJECT ------" << endl;

	// Call for projector operator
	bool ret = producer->GetNext(_record);
	if (ret) {
		_record.Project(keepMe, numAttsOutput, numAttsInput);
		return true;
	} else {
		return false;
	}
}

ostream& Project::print(ostream& _os) {
	_os << "PROJECT[ schemaIn: {";
	for(int i = 0; i < schemaIn.GetAtts().size();i++){
		_os << schemaIn.GetAtts()[i].name;
		if(i != schemaIn.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os<<"},schemaOut: {";
	for(int i = 0; i < schemaOut.GetAtts().size();i++){
		_os << schemaOut.GetAtts()[i].name;
		if(i != schemaOut.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os << "}]"<< endl;
	return _os << "\t\n\t--"<< *producer;
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

Join::~Join() {
		//printf("Deconstructor Join\n");
}

bool Join::NestedLoopJoin(Record& _record) {
	Record temp;

	if ()
}

bool Join::GetNext(Record& _record) {
	if (predicate) {
		
	}
}

ostream& Join::print(ostream& _os) {
	_os << "JOIN[ schemaLeft: {"; 
	for(int i = 0; i < schemaLeft.GetAtts().size();i++){
		_os << schemaLeft.GetAtts()[i].name;
		if(i != schemaLeft.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os << "},\n";
	for(int i = 0; i < push+1;i++){
		_os << "\t";
	} 
	_os << "schemaRight: {";
	for(int i = 0; i < schemaRight.GetAtts().size();i++){
		_os << schemaRight.GetAtts()[i].name;
		if(i != schemaRight.GetAtts().size()-1){
			_os << ", ";
		}
	}	
	_os << "},\n";
	for(int i = 0; i < push+1;i++){
		_os << "\t";
	} 
	_os << "schemaOut: {";
	for(int i = 0; i < schemaOut.GetAtts().size();i++){
		_os << schemaOut.GetAtts()[i].name;
		if(i != schemaOut.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os << "}]\tNumber of Tuples: " << size << endl; 
	for(int i = 0; i < push+1;i++){
		_os << "\t";
	}
 	_os <<"--"<<*right ;
 	for(int i = 0; i < push+1;i++){
		_os << "\t";
	}
 	return _os <<"--" << *left;
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {
	schema = _schema;
	producer = _producer;
}

DuplicateRemoval::~DuplicateRemoval() {
	//printf("Deconstructor DuplicateRemoval\n");
}


bool DuplicateRemoval::GetNext(Record& _record) {
	while(producer->GetNext(_record)) {
		stringstream temp;
		_record.print(temp, schema);
		_record.GetBits();
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
	_os << "DISTINCT[{";
	for(int i = 0; i < schema.GetAtts().size();i++){
		_os << schema.GetAtts()[i].name;
		if(i != schema.GetAtts().size()-1){
			_os << ", ";
		}
	}
	return _os<<"}]"<< "\n\t\n\t--" << *producer;
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	compute = _compute;
	producer = _producer;
}

Sum::~Sum() {
	//printf("Deconstructor Sum\n");
}

/*
	* Function: string GetType(), Type Apply(Record toMe, Int intResult, Double dblResult) 
*/

//TODO: Implement
bool Sum::GetNext(Record& _record) {
	


	double runningSum = 0;
	Type typeSum;
	Record tempRec;
	bool infiniteLoop = false;	
	char* bits = new char[1];
	while(producer->GetNext(tempRec)) {
		//Don't forget to set to 0
		int tempInt = 0;
		double tempDouble = 0;
		typeSum = compute.Apply(tempRec, tempInt, tempDouble);
		runningSum += tempInt + tempDouble;
		infiniteLoop = true;
	}
	//TODO: Change - Unsure
	if(infiniteLoop) {
		char* recordResult;
		if(typeSum == Float) {
			
			// _record.Consume(recordResult);
			// infiniteLoop = false;

			*((double *) bits) = runningSum;
			recordResult = new char[2*sizeof(int) + sizeof(double)];
			((int*) recordResult)[0] = sizeof(int) + sizeof(int) + sizeof(double);
			((int*) recordResult)[1] = sizeof(int) + sizeof(int);
			memcpy(recordResult+2*sizeof(int), bits, sizeof(double));
		}
		else {
			*((int *) bits) = (int)runningSum;
			recordResult = new char[3*sizeof(int)];
			((int*) recordResult)[0] = sizeof(int) + sizeof(int) + sizeof(int);
			((int*) recordResult)[1] = sizeof(int) + sizeof(int);
			memcpy(recordResult+2*sizeof(int), bits, sizeof(int));
		}
		_record.Consume(recordResult);
		//infiniteLoop = false;
		return true;
	}
	else {
		return false;
	}
	
}

ostream& Sum::print(ostream& _os) {
	 _os << "SUM:[ schemaIn: {";
	for(int i = 0; i < schemaIn.GetAtts().size();i++){
		_os << schemaIn.GetAtts()[i].name;
		if(i != schemaIn.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os<<"},schemaOut: {";
	for(int i = 0; i < schemaOut.GetAtts().size();i++){
		_os << schemaOut.GetAtts()[i].name;
		if(i != schemaOut.GetAtts().size()-1){
			_os << ", ";
		}
	}
	return _os << "}]" << "\n\t\n\t--" <<*producer;
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	groupingAtts = _groupingAtts; 
	compute = _compute;
	producer = _producer;	
	isFirst = true;
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute, FuncOperator* _parseTree, RelationalOp* _producer) {
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
	if (retType == Integer) {
		attrsType.push_back("INTEGER");
	}
	else if (retType == Float) {
		attrsType.push_back("FLOAT");
	}
	vector<string> attrsName;
	attrsName.push_back("sum");
	vector<unsigned int> attrsDist;
	attrsDist.push_back(0);
	Schema sumSchema(attrsName, attrsType, attrsDist);
	sumSchema.Append(_schemaIn);
	_schemaOut.Swap(sumSchema);
}

GroupBy::~GroupBy() {
	//printf("Deconstructor GroupBy\n");
}

//WTF Is this shit
//map with relational op usage
bool GroupBy::GetNext(Record& _record) {
	Type retType;
	if(isFirst == true) {
		isFirst = false;
		while(1) {
				Record * tmp = new Record();
				bool ret = this->producer->GetNext(*tmp);
				if(ret == false) break;
				if(tmp->GetSize() == 0) continue;
				++cnt;
				string value;
				//enum Type {Integer, Float, String, Name};
				if(groupingAtts.whichTypes[0] == Type::Integer) {
					char * val1 = tmp->GetColumn(groupingAtts.whichAtts[0]);//only support 1 group by attributes
					int val1Int = *((int *) val1);
					value = to_string(val1Int);
				} else if(groupingAtts.whichTypes[0] == Type::Float) {
					char * val1 = tmp->GetColumn(groupingAtts.whichAtts[0]);
					float val1Int = *((double *)  val1);
					value = to_string(val1Int);
				} else {
					char * val1 = tmp->GetColumn(groupingAtts.whichAtts[0]);
					string valueCharTmp(val1);
					// if(valueCharTmp == "Customer#000001359")
					//     break;
					value = valueCharTmp;
				}
				auto itgmap = gmap.find(value);
				if(itgmap != gmap.end()) {
					int sumIntArg = 0;
					double sumDoubleArg = 0.0;
					retType = compute.Apply(*tmp, sumIntArg, sumDoubleArg);
					if(retType == Type::Integer) {
						auto itSumInt = gmapSumInt.find(value);
						itSumInt->second += sumIntArg;
					} else if(retType == Type::Float) {
						auto itSumDouble = gmapSumDouble.find(value);
						itSumDouble->second += sumDoubleArg;
					}
					bool isExist = false;
					for(auto itdeque = itgmap->second->begin(); itdeque != itgmap->second->end(); ++itdeque) {
						if(groupingAtts.Run(**itdeque, *tmp) == 0) {
//							cout<<"line 839\n";
							isExist = true;
							break;
						}
					}
					if(isExist == false) {
						itgmap->second->push_back(tmp);
					}
				} else {
					int sumIntArg = 0;
					double sumDoubleArg = 0.0;
					retType = compute.Apply(*tmp, sumIntArg, sumDoubleArg);
					if(retType == Type::Integer) {
						gmapSumInt.insert(make_pair(value, sumIntArg));
					} else if(retType == Type::Float) {
						gmapSumDouble.insert(make_pair(value, sumDoubleArg));
					}
					deque<Record*> * dequeTmp = new deque<Record*>();
					dequeTmp->push_back(tmp);
					gmap.insert(make_pair(value, dequeTmp));
				}
			}
		//cout<<"line 1001 group by join :"<<cnt<<endl;
		for(auto it = gmap.begin(); it != gmap.end(); ++it) {
				string gname = it->first;
				deque<Record*> * dq = it->second;
				if(retType == Type::Integer) {
					string tmpNum;
					tmpNum = to_string(gmapSumInt.find(gname)->second);
					//record
					char* bits;
					char* space = new char[PAGE_SIZE];
					char* recSpace = new char[PAGE_SIZE];
					bits = NULL;
					int currentPosInRec = sizeof (int) * (2);
					for(int i = 0; i < tmpNum.length(); ++i) {
						space[i] = tmpNum[i];
					}
					((int *) recSpace)[1] = currentPosInRec;
					space[tmpNum.length()] = 0;
				//	vector<string> attrsType;
					*((int *) &(recSpace[currentPosInRec])) = atoi(space);
					currentPosInRec += sizeof (int);
			//		attrsType.push_back("INTEGER");
					((int *) recSpace)[0] = currentPosInRec;
					bits = new char[currentPosInRec];
					memcpy (bits, recSpace, currentPosInRec);
					delete [] space;
					delete [] recSpace;
					Record sumRecord;
					sumRecord.Consume(bits);
					auto gmapItems = gmap.find(gname);
					for(auto it = gmapItems->second->begin(); it != gmapItems->second->end(); ++it) {
						Record * tmp = new Record();
						tmp->AppendRecords(sumRecord, **it, 1, this->schemaIn.GetNumAtts());
						((Record*)(*it))->Swap(*tmp);
					}

				} else if(retType == Type::Float) {
					string tmpNum;
					tmpNum = to_string(gmapSumDouble.find(gname)->second);
					//record
					char* bits;
					char* space = new char[PAGE_SIZE];
					char* recSpace = new char[PAGE_SIZE];
					bits = NULL;
					int currentPosInRec = sizeof (int) * (2);
					for(int i = 0; i < tmpNum.length(); ++i) {
						space[i] = tmpNum[i];
					}
					((int *) recSpace)[1] = currentPosInRec;
					space[tmpNum.length()] = 0;
					*((double *) &(recSpace[currentPosInRec])) = atof (space);
					currentPosInRec += sizeof (double);
			//		attrsType.push_back("FLOAT");
					((int *) recSpace)[0] = currentPosInRec;
					bits = new char[currentPosInRec];
					memcpy (bits, recSpace, currentPosInRec);
					delete [] space;
					delete [] recSpace;
					Record sumRecord;
					sumRecord.Consume(bits);
					auto gmapItems = gmap.find(gname);
					for(auto it = gmapItems->second->begin(); it != gmapItems->second->end(); ++it) {
						Record * tmp = new Record();
						tmp->AppendRecords(sumRecord, **it, 1, this->schemaIn.GetNumAtts());
						((Record*)(*it))->Swap(*tmp);
//						auto ans = newGmap.find(gname);
//						if(ans != newGmap.end()) {
//							ans->second->push_back(tmp);
//						} else {
//							deque<Record*> * deqTmp = new deque<Record*>();
//							deqTmp->push_back(tmp);
//							newGmap.insert(make_pair(gname, deqTmp));
//						}
					}
				}
			}
	}

//	cout<<"newGmap:"<<newGmap.size()<<endl;

	//output gmap
	if(!gmap.empty()) {
		int count = 0;
		int size = gmap.size();
		for(auto it1 = gmap.begin(); it1 != gmap.end(); ++it1) {
			++count;
			if((count == size) && (it1->second->empty())) return false;
			if(!it1->second->empty()) {
				Record * output = it1->second->front();
				_record.Swap(*output);
				it1->second->pop_front();
				return true;
			}
		}
	}
}

RelationalOp* GroupBy::GetProducer() {
	return producer;
}


ostream& GroupBy::print(ostream& _os) {
	_os << "GROUP BY[ schemaIn: {";
	for(int i = 0; i < schemaIn.GetAtts().size();i++){
		_os << schemaIn.GetAtts()[i].name;
		if(i != schemaIn.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os<<"},schemaOut: {";
	for(int i = 0; i < schemaOut.GetAtts().size();i++){
		_os << schemaOut.GetAtts()[i].name;
		if(i != schemaOut.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os << "}]"<< endl;
	return _os << "\t\n\t--"<< *producer;
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {
	schema = _schema;
	outFile = _outFile;
	producer = _producer;
	outFile = _outFile;
	//Open the file stream
	out.open(outFile.c_str());
	//cout << "---- enter ------" << endl;

}

WriteOut::~WriteOut() {
	//printf("Deconstructor WriteOut\n");
	//if filestream is open, close it
	if(out.is_open()){
		out.close();
	}
}

bool WriteOut::GetNext(Record& _record) {
	//Get record from producer
//	cout << "---- WRITE OUT ------" << endl;
	/*
	bool ret = producer->GetNext(_record);
	if (ret)
	{	
		//Write to the outfile all the records matching the schema
		_record.print(out,schema);
		out << endl;
		//For demo only, need to comment out all other cout statments
		 _record.print(cout,schema);
		 cout << endl;
		return true;
	}
	else
	{		
		//Close the file stream
		out.close();
		return false;
	}
	*/

	//cout << "---- IN WRITE OUT ------" << endl;
	bool ret = producer->GetNext(_record);
	if(ret) {
	//	cout << "---- IF STATEMENT ------" << endl;

		//Lecture notes line below. 
		//outFile << _record.print(schema);
		_record.print(out, schema);
		out << endl;
		_record.print(cout, schema);
		cout << endl;
		return true;
	}
	else {
		out.close();
		return false;
	}
}

ostream& WriteOut::print(ostream& _os) {
	_os << "WRITE OUT [{";
	for(int i = 0; i < schema.GetAtts().size();i++){
		_os << schema.GetAtts()[i].name;
		if(i != schema.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os<<"}]"<< endl; 
	return _os << "\t--"<< *producer;
}

void QueryExecutionTree::ExecuteQuery(){
	Record rec;
	//cout << "---- EXECUTE QUERY ------" << endl;
	while(root->GetNext(rec)) {
		//This is enough
	}
}

ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	_os << "QUERY EXECUTION TREE " <<endl; 
	return _os << "--"<<*_op.root;
}