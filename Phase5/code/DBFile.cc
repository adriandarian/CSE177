#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;


DBFile::DBFile () : fileName("") {
}

DBFile::~DBFile () {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

//Lecture Notes
int DBFile::Create (char* f_path, FileType f_type) {
	fileType = f_type;
	return file.Open(0, f_path);	
}

//Modified from lecture notes - moved MoveFirst() call here
int DBFile::Open (char* f_path) {
	int o = file.Open(1, f_path);

	if (o == 0) {
		MoveFirst();
		return o;
	}
	//don't think this is needed
	return -1;
}

//Lecture notes, move MoveFirst() call to Open() from Load()
void DBFile::Load (Schema& schema, char* textFile) {
	FILE* fOpen = fopen(textFile, "r");
	Record rec;
	while (rec.ExtractNextRecord(schema, *fOpen)) {
		AppendRecord(rec);
	}
	file.AddPage(page, currPage);
	currPage++;
	page.EmptyItOut();
	fclose(fOpen);
}

int DBFile::Close () {
	return file.Close();
}

void DBFile::MoveFirst () {
	currPage = 0;
	file.GetPage(page, currPage);
}

void DBFile::AppendRecord (Record& rec) {
	if(!page.Append(rec)) {
		file.AddPage(page, currPage);
		currPage++;
		page.EmptyItOut();
		page.Append(rec);
	}
}

// Lecture Notes
int DBFile::GetNext (Record& rec) {
	int ret = page.GetFirst(rec);

	if (ret == true) {
		return true;
	} else {
		if (currPage == file.GetLength()) {
			return false;
		} else {
			file.GetPage(page, currPage);
			currPage++;
			ret = page.GetFirst(rec);
			return true;
		}
	}

}