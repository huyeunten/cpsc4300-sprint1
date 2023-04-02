#include "../sql-parser/src/SQLParser.h"
#include "../sql-parser/src/sqlhelper.h"
#include "db_cxx.h"
#include <iostream>
#include <cstring>
using namespace std;

const std::string QUIT = "quit";
const unsigned int BLOCK_SZ = 4096;
const char *MILESTONE1 = "milestone1.db";

std::string execute(hsql::SQLParserResult* query);

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cout << "Usage: ./milestone1 path" << std::endl;
        return -1;
    }

    std::string directory = argv[1];

    // maybe check if directory is valid?

    // Create database in directory
    // Path must be from root of directory (eg cpsc4300/data)
    const char *home = std::getenv("HOME");
	std::string envdir = std::string(home) + "/" + directory;

    DbEnv env(0U);
    env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

	Db db(&env, 0);
	db.set_message_stream(env.get_message_stream());
	db.set_error_stream(env.get_error_stream());
	db.set_re_len(BLOCK_SZ);
	db.open(NULL, MILESTONE1, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644);

    std::string response;
    while (response != QUIT) {
        std::cout << "SQL> ";
        getline(std::cin, response);

        char* responseArray = new char[response.length() + 1];
        strcpy(responseArray, response.c_str());

        hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(responseArray);

        if (result->isValid()) {
            std::string sql = execute(result);
            std::cout << sql << std::endl;
        }
        else if (response != QUIT) {
            std::cout << "Invalid SQL: " << response << std::endl;
        }

        delete [] responseArray;
        delete result;
    }
}

std::string execute(hsql::SQLParserResult* query) {
    std::string finalQuery = "";
    int n = query->size();
    //hsql::StatementType statementType = query->getStatement(0)->type();
    //std::cout << "Statement type is: " << statementType << std::endl;

    // TODO: turn query variable into formal sql query
    for (int i = 0; i < n; i++) {
        const hsql::SQLStatement* statement = query->getStatement(i);
        //hsql::printStatementInfo(statement);

        hsql::StatementType statementType = statement->type();
        std::cout << "Statement type is: " << statementType << std::endl;

        switch(statementType){
            case hsql::kStmtCreate: // create statement
                finalQuery += "CREATE TABLE ";
                // hsql::CreateType type = hsql::CreateType.kTable;
                // hsql::CreateStatement createStatement = hsql::CreateStatement(); // assume create statements are always creating a table
                // std::cout << "Table name: " << createStatement.tableName << std::endl;
                // std::cout << "File path: " << statement->filePath << std::endl;
                // std::cout << "columns: " << statement->columns << std::endl;
                break;
            case hsql::kStmtInsert: // insert statement
                finalQuery += "INSERT";
                break;
            case hsql::kStmtSelect:
                finalQuery += "SELECT";
                //hsql::SelectStatement selectStatement = hsql::SelectStatement();

                // hsql::printSelectStatementInfo(selectStatement, 1);

                //cout << "Where clause:" << endl;
                //hsql::printExpression(selectStatement.whereClause, 2);

                // std::cout << "Table:" << std::endl;
                // hsql::printTableRefInfo(selectStatement.fromTable, 2);
                // cout << "Columns to select:" << endl;
                // for (hsql::Expr* expr : selectStatement->selectList) hsql::printExpression(expr, 2);
                //     std::cout << "Columns we're selecting:" << std::endl;

                //for(hsql::Expr* expr : ((hsql::SelectStatement*)statement)->selectList)
                //     hsql::printExpression(expr, 2);

                //delete selectStatement;

                break;
            
        }
    }
    return finalQuery;
}