#include <string>
#include <vector>
#include <iostream>

#include<algorithm>
#include"limits.h"

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

using namespace std;


QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
}

QueryOptimizer::~QueryOptimizer() {
}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* _root) {
	// compute the optimal join order

	// working varaibles
	tables = _tables;
	predicate = _predicate;
	root = _root;
	int numTables = 0;
	unsigned long long numTuplesInTable = 0;
	vector<string> t_names;
	TableList *tableList = tables;

	while ( tableList != NULL) {
		string tableNameInTableList = tableList->tableName;
		t_names.push_back(tableNameInTableList);

		numTuplesInTable = pushDownSelection(tableNameInTableList);

		OptimizationTree *instance = new OptimizationTree;
		instance->leftChild = NULL;
		instance->rightChild = NULL;

		instance->tables.push_back(tableNameInTableList);
		instance->tuples.push_back(numTuplesInTable);
		instance->noTuples = numTuplesInTable;
		
		t_map[tableNameInTableList].size = numTuplesInTable;
		t_map[tableNameInTableList].cost = 0;
		t_map[tableNameInTableList].order = instance;

		numTables++;	
		tableList = tableList->next;
	}
	
	switch(numTables) {
		case 0:
		{
			root = NULL;
		}
			break;
		case 1:
		{
			*root = *t_map[tables->tableName].order;
		}
			break;
		case 2:
		{
			initializeTablePair();

			string table1;
			table1 = tables->tableName;

			string table2;
			table2 = tables->next->tableName;

			if (t_map.end() == t_map.find(table1+table2)) {
				*root = *t_map[table2+table1].order;
			} else {
				*root = *t_map[table1+table2].order;
			}
		}
			break;	
		default:
		{
		
			initializeTablePair();

			partitionTables(numTables, t_names);

			string t_key;
			t_key = getTableKey(t_names);

			*root = *t_map[t_key].order;

		}
	}
}

unsigned long long QueryOptimizer::pushDownSelection(string &tableName) {
	Schema schema;
	Record record;

	// obtain data from the catalog
	catalog->GetSchema(tableName, schema);

	unsigned long long numTuples = 0;
	unsigned int hh46;
	catalog->GetNoTuples(tableName, hh46);

	numTuples = hh46;
	t_map[tableName].schema = schema;

	//Extracing the predicates
	// Rangle queries
	CNF predicates;
	predicates.ExtractCNF(*predicate, schema, record);
	for (int i = 0; i < predicates.numAnds; i++) {
		switch (predicates.andList[i].op) {
			case Equals:
			{
				vector<Attribute> attributes = schema.GetAtts();

				if (predicates.andList[i].operand1 == Literal) {
					numTuples /= attributes[predicates.andList[i].whichAtt2].noDistinct;
				} else {
					numTuples /= attributes[predicates.andList[i].whichAtt1].noDistinct;
				}
			}
				break;
			default:
			{
				numTuples /= 3;
			}
		}
	}

	return numTuples;	
}

//find all pairs possible
void QueryOptimizer::initializeTablePair() {
	TableList *tableList1 = tables;
	while (tableList1 != NULL) {
		TableList *tableList2 = tableList1->next;
		while (tableList2 != NULL) {
			string tableName1;
			tableName1 = tableList1->tableName;
			string tableName2;
			tableName2 = tableList2->tableName;

			Schema schema1;
			schema1 = Schema(t_map[tableName1].schema);
			Schema schema2;
			schema2 = Schema(t_map[tableName2].schema);

			vector<Attribute> attribute1;
			attribute1 = schema1.GetAtts();			
			vector<Attribute> attribute2;
			attribute2 = schema2.GetAtts();

			vector<string> tableNames;
			tableNames.push_back(tableName1);
			tableNames.push_back(tableName2);

			string t_key;
			t_key = getTableKey(tableNames);
			t_map[t_key].size = t_map[tableName2].size * t_map[tableName1].size;

			//Join Cardinality Estimates
			CNF predicates;
			predicates.ExtractCNF(*predicate, schema1, schema2);

			vector<Attribute> attribute3;
			attribute3 = schema1.GetAtts();

			vector<Attribute> attribute4;
			attribute4 = schema2.GetAtts();

			for (int i = 0; i < predicates.numAnds; i++) {
				if (predicates.andList[i].operand1 == Right) {
					t_map[t_key].size /= max(attribute4[predicates.andList[i].whichAtt1].noDistinct, attribute3[predicates.andList[i].whichAtt2].noDistinct);
				}

				if (predicates.andList[i].operand1 == Left) {
					t_map[t_key].size /= max(attribute3[predicates.andList[i].whichAtt1].noDistinct, attribute4[predicates.andList[i].whichAtt2].noDistinct);
				}
			}
			t_map[t_key].cost = 0;


			// Tree part
			OptimizationTree *tempTree = new OptimizationTree;

			tempTree->leftChild = t_map[tableName1].order;
			t_map[tableName1].order->parent = tempTree;

			tempTree->rightChild = t_map[tableName2].order;
			t_map[tableName2].order->parent = tempTree;

			tempTree->parent = NULL;
			t_map[t_key].order = tempTree;

			tempTree->tables = tableNames;
			tempTree->tuples.push_back(t_map[tableName1].size);
			tempTree->tuples.push_back(t_map[tableName2].size);
			tempTree->noTuples = t_map[t_key].size;

			schema1.Append(schema2);
			t_map[t_key].schema = schema1;


			tableList2 = tableList2->next;
		}
		tableList1 = tableList1->next;
	}
}

void QueryOptimizer::partitionTables(int _numTables, vector<string> &_tableNames) {
	//working varaible
	int numTables = _numTables;

	string t_key;
	t_key = getTableKey(_tableNames);

	if (t_map.end() == t_map.find(t_key)) {

		// Calculating permutations
		vector<int> tempVector;
		for (int i = 0; i < _numTables; i++) {
			tempVector.push_back(i);
		}

		// Generating permutations using Heap's Algorithm
		vector<vector<int>> permutations;
		generatePermutation(permutations, tempVector, numTables);

		unsigned long long cost = 0;
		unsigned long long minCost = UINT_MAX;
		unsigned long long permutationSize = 0;

		permutationSize = factorial(numTables);

		for (int i = 0; i < permutationSize; i++) {
			vector<int>permute = permutations[i];
			for (int j = 1; j < numTables; j++) {
				// Left
				vector<string> L, tempL;
				for (int m = 0; m < j; m++) {
					tempL.push_back(_tableNames[permute[m]]);
				}
				L = tempL;
				string leftKey;
				leftKey = getTableKey(L);
				if (t_map.end() == t_map.find(leftKey)) {
					partitionTables(j, L);
				}

				// Right
				vector<string> R, tempR;
				for (int n = j; n < numTables; n++) {
					tempR.push_back(_tableNames[permute[n]]);
				}
				R = tempR;
				string rightKey;
				rightKey = getTableKey(R);

				if (t_map.end() == t_map.find(rightKey)) {

					partitionTables(numTables-j, R);
				}

				cost = t_map[leftKey].cost + t_map[rightKey].cost;

				if (j != 1) {
					cost += t_map[leftKey].size;
				}

				if (j != numTables-1) {
					cost += t_map[rightKey].size;
				}

				if (minCost > cost) {
					minCost = cost;

					Schema schema1;
					schema1 = Schema(t_map[leftKey].schema);

					Schema schema2;
					schema2 = Schema(t_map[rightKey].schema);

					//Join Cardinality Estimates
					unsigned long long t_size = t_map[leftKey].size * t_map[rightKey].size;
					CNF predicates;
					predicates.ExtractCNF(*predicate, schema1, schema2);
					vector<Attribute> attribute3;
					attribute3 = schema1.GetAtts();
					vector<Attribute> attribute4;
					attribute4 = schema2.GetAtts();

					for (int i = 0; i < predicates.numAnds; i++) {
						if (predicates.andList[i].operand1 == Right) {
							t_size /= max(attribute4[predicates.andList[i].whichAtt1].noDistinct, attribute3[predicates.andList[i].whichAtt2].noDistinct);
						}

						if (predicates.andList[i].operand1 == Left) {
							t_size /= max(attribute3[predicates.andList[i].whichAtt1].noDistinct, attribute4[predicates.andList[i].whichAtt2].noDistinct);
						}
					}
					t_map[t_key].size = t_size;
					t_map[t_key].cost = minCost;
					schema1.Append(schema2);
					t_map[t_key].schema = schema1;


					OptimizationTree *opTree = new OptimizationTree;

					opTree->leftChild = t_map[leftKey].order;
					t_map[leftKey].order->parent = opTree;
					vector<int> leftChildTuples = vector<int>(t_map[leftKey].order->tuples);

					opTree->rightChild = t_map[rightKey].order;
					t_map[rightKey].order->parent = opTree;
					vector<int> rightChildTuples = vector<int>(t_map[rightKey].order->tuples);

					opTree->parent = NULL;

					leftChildTuples.insert(leftChildTuples.end(), rightChildTuples.begin(), rightChildTuples.end());
					opTree->tuples = leftChildTuples;

					opTree->tables = _tableNames;
					opTree->noTuples = t_map[t_key].size;
					t_map[t_key].order = opTree;
				}
			}
		}
	}
}

string QueryOptimizer::getTableKey(vector<string> _tableNames) {
	string tableKey = "";
	sort(_tableNames.begin(), _tableNames.end());

	vector<string>::iterator iter = _tableNames.begin();
	while (iter != _tableNames.end()) {
		tableKey += *iter;
		++iter;
	}

	return tableKey;
}

void QueryOptimizer::generatePermutation(vector<vector<int>> &_permutations, vector<int> &_tempVector, int size) {

	if (size == 1) {
		vector<int> anotherTempVector(_tempVector);
		_permutations.push_back(anotherTempVector);

		return;
	}

	for (int i = 0; i < size; i++) {
		generatePermutation(_permutations, _tempVector, size-1);

		if (size % 2 == 1) {
			int temp;
			temp = _tempVector[0];
			_tempVector[0] = _tempVector[size-1];
			_tempVector[size-1] = temp;
		} else {
			int temp;
			temp = _tempVector[i];
			_tempVector[i] = _tempVector[size-1];
			_tempVector[size-1] = temp;
		}
	}
}

unsigned long long QueryOptimizer::factorial(int _numTables) {
	int numTables = _numTables;
	unsigned long long result = 0;

	if (numTables == 0) {
		return 1;
	}
	return (numTables * factorial(numTables-1));
}
