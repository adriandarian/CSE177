#include <string>
#include <vector>
#include <iostream>
#include <algorithm>    // std::max

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

using namespace std;

QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
}

QueryOptimizer::~QueryOptimizer() {
}

string QueryOptimizer::getPredicateTable(string _predicateValue) {
	vector<string>allTables;
	string predicateTableName;
	catalog->GetTables(allTables);
	for (int i = 0; i < allTables.size(); i++) {
		Schema tableSchema;
		catalog->GetSchema(allTables[i], tableSchema);
		int tableAttributeSize = tableSchema.GetNumAtts();
			for (int j = 0; j < tableAttributeSize; j++) {
				if (tableSchema.GetAtts()[j].name == _predicateValue) {
					predicateTableName = allTables[i];
				}
			}
	}

	return predicateTableName;
}

vector<string> QueryOptimizer::convertTableListToVector(TableList *_tables) {
	vector<string> tables;
	TableList* tempTables; 
	tempTables = _tables;
	while (tempTables != NULL) {
		tables.push_back(tempTables->tableName);
		tempTables = tempTables->next;
	}
		return tables;
}

void QueryOptimizer::printNode(OptimizationTree* node) {
	cout << "Node ID " <<  node->id  << ":" << endl;
	cout << "----------Tables----------" << endl;
	for (int i = 0; i < node->tables.size(); i++) {
		cout << node->tables[i] << endl;
	}
	cout << "----------Tuples----------" << endl;
	for (int i = 0; i < node->tuples.size(); i++) {
		cout << node->tuples[i] << endl;
	}

	cout << "----------Number of Tuples for Node----------" << endl;
	cout << node->noTuples << endl;

	cout << "----------Parents and Children----------" << endl;
	if (node->parent == NULL) {
		cout << "Parent is NULL" << endl;
	}
	else {
		cout << "Parent of Node: " << node->parent->id << endl;
	}

	if (node->leftChild == NULL) {
		cout << "LeftChild is NULL" << endl;
	}
	else {
		cout << "LeftChild of Node: " << node->leftChild->id << endl;
	}

	if (node->rightChild == NULL) {
		cout << "RightChild is NULL" << endl;
	}
	else {
		cout << "RightChild of Node: " << node->rightChild->id << endl;
	}

	cout << endl;

}

void QueryOptimizer::printAllNodes(vector<OptimizationTree*> myNodes) {
	for (int i = 0; i < myNodes.size(); i++) {
		printNode(myNodes[i]);
	}
}

void QueryOptimizer::applyPushDownSelections(vector<OptimizationTree*>initialNodes, AndList*_predicate) {
	AndList* tempPredicates;
	tempPredicates = _predicate;
	
	while (tempPredicates != NULL) {
		Operand* left = tempPredicates->left->left;
		Operand* right = tempPredicates->left->right;
		//case where we have a attribute equal to value 
		//Ex: s_acctbal > 2500
		if (left->code != right->code) {
			string attributeName;
			//check which one of the sides is the attribute
			if (left->code == NAME) {
				attributeName = left->value;
			}
			else {
				attributeName = right->value;
			}
			string tableName = getPredicateTable(attributeName);
			//loop through the notes vector
			for (int i = 0; i < initialNodes.size(); i++) {
				//check if the tableName for the predicate is equal to one of the table names for the nodes
				if (tableName == initialNodes[i]->tables[0]) {
					//check the sign < > or = and determine how to divide the tuples
					if (tempPredicates->left->code == GREATER_THAN || tempPredicates->left->code == LESS_THAN) {
						initialNodes[i]->tuples[0] = initialNodes[i]->tuples[0] / 3;
						initialNodes[i]->noTuples = initialNodes[i]->noTuples / 3;
					}
					else {
						unsigned int attributeDistincts;
						catalog->GetNoDistinct(tableName, attributeName, attributeDistincts);
						initialNodes[i]->tuples[0] = initialNodes[i]->tuples[0] / attributeDistincts;
						initialNodes[i]->noTuples = initialNodes[i]->noTuples / attributeDistincts;
					}
				}
			}
		}
		tempPredicates = tempPredicates->rightAnd;
	}
}

void QueryOptimizer::printJoin(joins*tree) {
	cout << "----------------Left of Join----------------" << endl;
	cout << "Table: " << tree->leftTable << endl;
	cout << "Attribute: " << tree->leftAttribute << endl;

	cout << "----------------Right of Join----------------" << endl;
	cout << "Table: " << tree->rightTable << endl;
	cout << "Attribute: " << tree->rightAttribute << endl;

}

void QueryOptimizer::printAllJoins(vector<joins*> join) {
	if (join.size() == 0) {
		cout << "There were no joins for this query" << endl;
	}
	else {
		int joinValue = 1;
		for (int i = 0; i < join.size(); i++) {
			cout << "------------------------------------Join #" << joinValue++ << "------------------------------------" << endl;
			printJoin(join[i]);
		}
	}
}

void QueryOptimizer::getAllJoins(vector<joins*>&allJoins, AndList*_predicate) {
	AndList* tempPredicates;
	tempPredicates = _predicate;
	while (tempPredicates != NULL) {
		Operand* left = tempPredicates->left->left;
		Operand* right = tempPredicates->left->right;
		//case where we have a attribute equal to another attribute 
		//Ex: p_partkey = ps_parket
		if (left->code == right->code) {
			//cout << "There is a JOIN here" << endl;
			joins* newJoin = new joins();
			newJoin->leftAttribute = left->value;
			newJoin->leftTable = getPredicateTable(left->value);
			newJoin->rightAttribute = right->value;
			newJoin->rightTable = getPredicateTable(right->value);
			allJoins.push_back(newJoin);
		}
		else {
			//cout << "This is a Push-Down Predicate" << endl;
		}
		tempPredicates = tempPredicates->rightAnd;
	}
}

void QueryOptimizer::estimateMinCostOfJoins(vector<OptimizationTree*> initialNodes, vector<joins*>allJoins, long &minCost, int &index) {
	unsigned int estimationCost;
	bool initializedMinCost = false;

	for (int i = 0; i < allJoins.size(); i++) {
		unsigned int leftAttributeDistincts;
		unsigned int rightAttributeDistincts;
		unsigned int rightTableSize;
		unsigned int leftTableSize;
		for (int j = 0; j < initialNodes.size(); j++) {
			for (int k = 0; k < initialNodes[j]->tables.size(); k++) {
				if (allJoins[i]->leftTable == initialNodes[j]->tables[k]) {
					catalog->GetNoDistinct(allJoins[i]->leftTable, allJoins[i]->leftAttribute, leftAttributeDistincts);
					leftTableSize = initialNodes[j]->noTuples;	
				}
				else if (allJoins[i]->rightTable == initialNodes[j]->tables[k]) {
					catalog->GetNoDistinct(allJoins[i]->rightTable, allJoins[i]->rightAttribute, rightAttributeDistincts);
					rightTableSize = initialNodes[j]->noTuples;
					
				}
				
			}
		}
		estimationCost = (float)leftTableSize/ max(leftAttributeDistincts, rightAttributeDistincts)*rightTableSize;
		if (!initializedMinCost) {
			minCost = estimationCost;
			initializedMinCost = true;
		}
		if (estimationCost < minCost) {
			minCost = estimationCost;
			index = i;
		}
	}

}

bool contains(string value, vector<string> myVector) {
	for (int i = 0; i < myVector.size(); i++) {
		if (myVector[i] == value) {
			return true;
		}
	}

	return false;
}

void mergeNodeInfo(OptimizationTree* joinedNode, OptimizationTree* oldNode) {
	for (int j = 0; j < oldNode->tables.size(); j++) {
		joinedNode->tables.push_back(oldNode->tables[j]);
	}
	for (int j = 0; j < oldNode->tuples.size(); j++) {
		joinedNode->tuples.push_back(oldNode->tuples[j]);
	}
}

void postOrderTraversal(OptimizationTree* _root) {

	if (_root == NULL) {
		return;
	}

	postOrderTraversal(_root->leftChild);
	postOrderTraversal(_root->rightChild);
	cout << _root->id << endl;


}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* _root) {

	vector<OptimizationTree*>initialNodes;
	vector<string>allTables;
	int nodeID = 1;
	allTables = convertTableListToVector(_tables);

	//first stage is to simply create all the initial nodes
	for (int i = 0; i < allTables.size(); i++) {
		OptimizationTree* node = new OptimizationTree();
		unsigned int noTuples;
		node->id = nodeID++; //nodeID++ basically increaments to nodeID after it sets it
		node->tables.push_back(allTables[i]);
		catalog->GetNoTuples(allTables[i], noTuples);
		node->tuples.push_back(noTuples);
		node->noTuples = noTuples;
		node->parent = NULL;
		node->leftChild = NULL;
		node->rightChild = NULL;
		node->alreadyJoined = false;
		initialNodes.push_back(node);
	}

	//second stage is now to go through predicates and figure out which ones have a push down selection
	//then update the nodes accordingly 
	applyPushDownSelections(initialNodes, _predicate);
	
	//third stage is now to work on the ordering of the joins  
	vector<joins*> allJoins;
	//look at all joins I need to do by checking predicates and save them somewhere 
	getAllJoins(allJoins, _predicate);
	//estimate cost of each one

	while (allJoins.size() != 0) {
		long minCost = 0;
		int index = 0;
		estimateMinCostOfJoins(initialNodes, allJoins, minCost, index);
		OptimizationTree* joinedNode = new OptimizationTree();
		joinedNode->id = nodeID++;
	
		for (int i = 0; i < initialNodes.size(); i++) {
			if (contains(allJoins[index]->leftTable, initialNodes[i]->tables) && initialNodes[i]->alreadyJoined == false) {
				mergeNodeInfo(joinedNode, initialNodes[i]);
				initialNodes[i]->parent = joinedNode;
				initialNodes[i]->alreadyJoined = true;
				joinedNode->leftChild = initialNodes[i];
				//cout << "#################### Size " << allJoins.size() << " Index: " << index << " " << allJoins[index]->leftTable << "/" << allJoins[index]->leftAttribute << " IS IN INITIAL NODE ID: " << initialNodes[i]->id << endl;
				break;
			}
		}

		for (int i = 0; i < initialNodes.size(); i++) {
			if (contains(allJoins[index]->rightTable, initialNodes[i]->tables) && initialNodes[i]->alreadyJoined == false) {
				mergeNodeInfo(joinedNode, initialNodes[i]);
				initialNodes[i]->parent = joinedNode;
				initialNodes[i]->alreadyJoined = true;
				joinedNode->rightChild = initialNodes[i];
				break;
			}
		}

		joinedNode->noTuples = minCost;
		joinedNode->parent = NULL;
		joinedNode->alreadyJoined = false;
		initialNodes.push_back(joinedNode);
		//printAllNodes(initialNodes);
		allJoins.erase(allJoins.begin() + index);
		//printAllJoins(allJoins);
	}

	*_root = *initialNodes[initialNodes.size() - 1];
	//printAllNodes(initialNodes);
	//postOrderTraversal(_root);


	//choose the lowest and create a new node for that join (absorb the two child nodes into one. combine tables and tuples vector and mark children) 	
}