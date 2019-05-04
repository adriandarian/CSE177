#include <string>
#include <vector>
#include <iostream>
#include <algorithm>
#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

using namespace std;


QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
}

QueryOptimizer::~QueryOptimizer() {
	for(int i = 0; i < everything.size();i++){
		delete everything[i];
	}
}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* _root) {
	// compute the optimal join order
	TableList* temp = _tables;
	//Initialize all the basic nodes in the tree
	while(temp!=NULL){
		Schema schema;
		string tName = temp->tableName;
		if(!catalog->GetSchema(tName,schema)){
			return;
			//Shouldn't happen since we've confirmed it exists
		}
		unsigned int numTint;
		unsigned long int numT;
		if(!catalog->GetNoTuples(tName,numTint)){
			return;
			//Again, shouldn't happen
		}
		numT = numTint;
		//Calculating push down
		numT = calcPushDown(_predicate,schema,numT);

		OptimizationTree* opTree = new OptimizationTree;
		opTree->size = numT;
		opTree->tables.push_back(tName);
		opTree->leftChild = NULL;
		opTree->rightChild = NULL;
		opTree->parent =NULL;
		opTree->schema = schema;
		tvec.push_back(opTree);
		everything.push_back(opTree);
		temp = temp->next;						

	}
	// cout << "Check point 1, tvec values"<< endl;
	// for(int i = 0; i < tvec.size();i++){
	// 	cout << tvec[i]->tables[0] << " " << tvec[i]->size << endl;
	// }
	//Prep the tree by finding the first 'root'
	if(tvec.size() == 1){
		//cout << "exit" << endl;
		root = tvec[0];
		*_root = *tvec[0];
	}else{
		// cout << "Initialize" << endl;
		vector<OptimizationTree*> jNodes;
		for(int i = 0; i < tvec.size();i++){
			for(int j = 0; j < tvec.size();j++){
				if(i!= j){
					OptimizationTree* tO1 = tvec[i];
					OptimizationTree* tO2 = tvec[j];
					
					Schema schema1 = tvec[i]->schema;
					Schema schema2 = tvec[j]->schema;
					vector<Attribute> a1 = schema1.GetAtts();
					vector<Attribute> a2 = schema2.GetAtts();
					vector<string> tNames = tvec[i]->tables;
					tNames.insert(tNames.end(),tvec[j]->tables.begin(),tvec[j]->tables.end());
					int tempS = tvec[i]->size*tvec[j]->size;

					//Estimate Join Cardinality

					tempS = EstJoin(_predicate,schema1,schema2,tempS);
					
					OptimizationTree* opTree = new OptimizationTree;
					opTree->tables = tNames;
					opTree->size = tempS;
					opTree->leftChild = tO1;
					opTree->rightChild = tO2;
					opTree->parent = NULL;
					opTree->schema = schema1;
					opTree->schema.Append(schema2);
					jNodes.push_back(opTree);
					everything.push_back(opTree);
					// cout << "[";
					// for(int i = 0 ; i < opTree->tables.size();i++){
					// 	cout << opTree->tables[i];
					// 	if(i!= opTree->tables.size()-1){
					// 		cout << ", ";
					// 	}
					// }
					// cout <<"]" << " --"<< opTree->size << endl;

				}

				//temp2 = temp2->next;
			}
			//stemp = temp->next;
		}
		// cout << "Check point 1.5, jNodes values"<< endl;
		// for(int i = 0; i < jNodes.size();i++){
		// 	cout << "[";
		// 	for(int j = 0; j < jNodes[i]->tables.size();j++){
		// 		cout << jNodes[i]->tables[j] <<", ";
		// 	}
		// 	cout << "] --" << jNodes[i]->size << endl;
		// }		
		int min = jNodes[0]->size;
		int index = 0;
		for(int i = 1; i < jNodes.size();i++){
			if(jNodes[i]->size < min){
				min = jNodes[i]->size;
				index = i;
			}
		}
		OptimizationTree* tRoot = new OptimizationTree;
		tRoot->tables = jNodes[index]->tables;
		tRoot->size = jNodes[index]->size;
		tRoot->parent = NULL;
		tRoot->schema = jNodes[index]->schema;
		OptimizationTree* tO1 = jNodes[index]->leftChild;
		OptimizationTree* tO2 = jNodes[index]->rightChild;
		tO1->parent = tRoot;
		tO2->parent = tRoot;
		tRoot->leftChild = tO1;
		tRoot->rightChild = tO2;
		// for(int i = 0; i < tRoot->schema.GetAtts().size();i++){
		// 	cout << tRoot->schema.GetAtts()[i].name;
		// 	if(i != tRoot->schema.GetAtts().size()-1){
		// 		cout << ", ";
		// 	}
		// }
		// cout << endl;	
		root =tRoot;

		cvec.push_back(tO1);
		cvec.push_back(tO2);

		jNodes.clear();

		vector<OptimizationTree*> copy = tvec;
		tvec.clear();
		for(int i = 0; i < copy.size();i++){
			if(tvec[i]!=tO1 && tvec[i]!= tO2){
				tvec.push_back(copy[i]);
			}
		}
		// cout << "Check point 2"<< endl;
		// for(int i = 0; i < tvec.size();i++){
		// 	cout << tvec[i]->tables[0] << endl;
		// }
		//Now we actually build the tree
		int wow = 0;
		while(!tvec.empty()){
			//cout << "----------------------------------------------------------------------------------------calc joins take: "<< wow << endl;
			for(int i = 0; i < tvec.size();i++){
				Schema schema1 = root->schema;
				Schema schema2 = tvec[i]->schema;

				OptimizationTree* tO3 = tvec[i];
				vector<string> tNames = root->tables;
				tNames.insert(tNames.end(),tvec[i]->tables.begin(),tvec[i]->tables.end());
				unsigned long int tempS = root->size*tvec[i]->size;
				//cout << "**********************************"<<root->size << " "<<tvec[i]->size << endl;
				tempS = EstJoin(_predicate,schema1,schema2,tempS);

				OptimizationTree* opTree = new OptimizationTree;
				opTree->tables = tNames;
				opTree->size = tempS;
				opTree->leftChild = root;
				opTree->rightChild = tO3;
				opTree->parent = NULL;
				opTree->schema = schema1;
				opTree->schema.Append(schema2);
				jNodes.push_back(opTree);
				everything.push_back(opTree);
				// cout << "[";
				// for(int i = 0 ; i < opTree->tables.size();i++){
				// 	cout << opTree->tables[i];
				// 	if(i!= opTree->tables.size()-1){
				// 		cout << ", ";
				// 	}
				// }
				// cout <<"]" << " --"<< opTree->size << endl;			
				
			}
			min = jNodes[0]->size;
			index = 0;
			// cout << "join" << endl;
			// cout << jNodes[0]->schema << endl;
			// cout << jNodes[0]->size << endl;			
			for(int i = 1; i < jNodes.size();i++){
				// cout << jNodes[i]->schema << endl;
				// cout << jNodes[i]->size << endl;
				if(jNodes[i]->size < min){
					min = jNodes[i]->size;
					index = i;
				}
			}
			OptimizationTree* op = new OptimizationTree;
			op->tables = jNodes[index]->tables;
			op->size = jNodes[index]->size;
			op->parent = NULL;
			op->schema = jNodes[index]->schema;
			OptimizationTree* tO3 = jNodes[index]->rightChild;
			root->parent = op;
			tO3->parent = op;
			op->leftChild = root;
			op->rightChild = tO3;
			// for(int i = 0; i < op->leftChild->schema.GetAtts().size();i++){
			// 	cout << op->leftChild->schema.GetAtts()[i].name;
			// 	if(i != op->leftChild->schema.GetAtts().size()-1){
			// 		cout << ", ";
			// 	}
			// }
			// cout << endl;
			// for(int i = 0; i < op->rightChild->schema.GetAtts().size();i++){
			// 	cout << op->rightChild->schema.GetAtts()[i].name;
			// 	if(i != op->rightChild->schema.GetAtts().size()-1){
			// 		cout << ", ";
			// 	}
			// }
			// cout << endl;			
			// for(int i = 0; i < op->schema.GetAtts().size();i++){
			// 	cout << op->schema.GetAtts()[i].name;
			// 	if(i != op->schema.GetAtts().size()-1){
			// 		cout << ", ";
			// 	}
			// }
			// cout << endl;			
			// cout << "[";
			// for(int i = 0 ; i < op->tables.size();i++){
			// 	cout << op->tables[i];
			// 	if(i!= op->tables.size()-1){
			// 		cout << ", ";
			// 	}
			// }
			// cout <<"]" << " --"<< op->size << endl;				
			// cout << endl;
			root = op;

			cvec.push_back(tO3);

			jNodes.clear();
			copy = tvec;
			tvec.clear();
			for(int i = 0; i < copy.size();i++){
				//cout << copy[i]->size << " " << op->leftChild->size << " " << tO3->size << endl;
				if(copy[i]!=op->leftChild){
					if(copy[i]!= tO3){
						//cout << "inside "<< copy[i]->size << " " << op->leftChild->size << " " << tO3->size << endl;
						tvec.push_back(copy[i]);
					}
				}
			}
			wow++;		
		}
		*_root = *root;
	}
	//print();
	//postOrder(_root);
	
}
unsigned long int QueryOptimizer::calcPushDown(AndList* _predicate,Schema schema, unsigned long int num){
	//Need to get CNF again, just like compiler
	int numT = num;
	CNF cnf;
	Record rec;
	if(cnf.ExtractCNF(*_predicate,schema,rec)== -1){
		exit(-1);
		//Shouldn't happen
	}
	for(int i = 0; i < cnf.numAnds;i++){
		//Go through the Comparison array
		//enum CompOperator {LessThan, GreaterThan, Equals};
		if(cnf.andList[i].op == Equals){
			//T(S) = T(R)/V(R,A)
			vector<Attribute> a;
			//If the left operand is the attribute
			//enum Target {Left, Right, Literal};
			if(cnf.andList[i].operand1 != Literal){
				a = schema.GetAtts();
				numT = numT/a[cnf.andList[i].whichAtt1].noDistinct;
			}else{
				a = schema.GetAtts();
				numT = numT/a[cnf.andList[i].whichAtt2].noDistinct;
			}
		}else{
			//T(S) = T(R)/3
			numT/=3;
		}
	}
	return numT;	
}

unsigned long int QueryOptimizer::EstJoin(AndList* _predicate, Schema schema1, Schema schema2, unsigned long int num){
	unsigned long int tempS = num;
	//Estimate Join Cardinality, use CNF(schema,schema);
	vector<Attribute> a1 = schema1.GetAtts();
	vector<Attribute> a2 = schema2.GetAtts();
	CNF cnf;
	if(cnf.ExtractCNF(*_predicate,schema1,schema2) == -1){
		exit(-1);
	}
	// cout << "==============================================" << endl;
	// cout << schema1<<endl;
	// cout << schema2<<endl;
	// cout << "Before: "<< tempS << endl;

	for(int k = 0; k < cnf.numAnds;k++){
		if(cnf.andList[k].operand1 == Left){
			//cout << "Left Max: " << max(a1[cnf.andList[k].whichAtt1].noDistinct,a2[cnf.andList[k].whichAtt2].noDistinct) << endl;
			tempS/=max(a1[cnf.andList[k].whichAtt1].noDistinct,a2[cnf.andList[k].whichAtt2].noDistinct); 
		}else if(cnf.andList[k].operand1 == Right){
			//cout << "Right Max: " << max(a2[cnf.andList[k].whichAtt1].noDistinct,a1[cnf.andList[k].whichAtt2].noDistinct) << endl;
			tempS/=max(a2[cnf.andList[k].whichAtt1].noDistinct,a1[cnf.andList[k].whichAtt2].noDistinct); 
		}
	}
	// cout << schema1<<endl;
	// cout << schema2<<endl
	//cout << cnf.numAnds << endl;
	//cout <<tempS<<endl;
	return tempS;

}

void QueryOptimizer::postOrder(OptimizationTree* node){
	if(node == NULL){
		return;
	}
	postOrder(node->leftChild);
	postOrder(node->rightChild);
	cout << "JOIN ORDER" << endl;
	cout << "[";
	for(int i = 0; i < node->tables.size();i++){
		cout << node->tables[i];
		if(i != node->tables.size()-1){
			cout << ", ";
		}
	}
	cout << "] --"<< node->size<<endl;
}
void QueryOptimizer::print(){
	postOrder(root);
}
