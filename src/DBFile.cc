#include <string>
#include <iostream>
#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"
#include <fstream>
using namespace std;

DBFile::DBFile() : fileName("")
{
		currPage = 0;
}

DBFile::~DBFile()
{
}

DBFile::DBFile(const DBFile &_copyMe) : file(_copyMe.file), fileName(_copyMe.fileName) {}

DBFile &DBFile::operator=(const DBFile &_copyMe)
{
	// handle self-assignment first
	if (this == &_copyMe)
		return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

int DBFile::Create(char *f_path, FileType f_type)
{
	return file.Open(0, f_path);
}

int DBFile::Open(char *f_path)
{
	//Set the file name used for printing in Scan::print
	fileName = f_path;
	//Open the file
	int o = file.Open(1, f_path);
	//cout << f_path << endl;
	//Checking if it opened successfully
	if (o == 0)
		MoveFirst();
	return o;
}

void DBFile::Load(Schema &schema, char *textFile)
{
	//Point the file to the first page
	MoveFirst();
	//Open the textfile you are loading data from
	FILE *f = fopen(textFile, "r");
	//A record object to extract the data into
	Record rec;
	//Extract the data from the file, for this schema, into the record
	while (rec.ExtractNextRecord(schema, *f))
	{
		AppendRecord(rec);
	}
	//Add the last page of data
	file.AddPage(page, currPage++);
	//page.EmptyItOut();
}

int DBFile::Close()
{
	//Close the file
	file.Close();
}

void DBFile::MoveFirst()
{
	//Point the page to 0
	currPage = 0;
	//Get the page at currPage
	file.GetPage(page, currPage);
}

void DBFile::AppendRecord(Record &rec)
{
	//append the record to the page, also check if the data has reached the max page size
	int max = page.Append(rec);
	//Checking to see if data has reached max page size
	while (max == 0)
	{
		//While the data reaches the max size, add pages
		file.AddPage(page, currPage++);
		//Empty the page in order to clear the data so it can be readded
		page.EmptyItOut();
		//Continuing to check until it no longer reaches the max size
		max = page.Append(rec);
	}
}

int DBFile::GetNext(Record &rec)
{
	//cout << currPage << endl;
	//Get the first record of the page
	//cout << "---- DB NEXT ------" << endl;

	int ret = page.GetFirst(rec);
	if (ret)
	{
		//success
		return 0;
	}
	else
	{
		//Check if you've reached the end of the file
		if (currPage == file.GetLength())
		{
			return -1;
		}
		else
		{
			//Move onto the next page
			file.GetPage(page, currPage++);
			//Get the first record of the page
			ret = page.GetFirst(rec);
			return ret-1;
		}
	}
}

string DBFile::GetFile()
{
	//Get the file name for printing
	//Also set the page to 0, so we can start getting the data from the first page, just in case
	currPage = 0;
	return fileName;
}