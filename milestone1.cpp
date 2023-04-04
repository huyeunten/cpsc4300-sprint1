#include "../sql-parser/src/SQLParser.h"
#include "../sql-parser/src/sqlhelper.h"
#include "db_cxx.h"
#include <iostream>
#include <cstring>
#include <sstream>
#include <cstdio>
using namespace std;

const std::string QUIT = "quit";
const unsigned int BLOCK_SZ = 4096;
const char *MILESTONE1 = "milestone1.db";

std::string execute(hsql::SQLParserResult* query, std::string response);
std::string parseCreate(std::string response);
string parseTableRef(hsql::TableRef* tableRef);
string parseSelect(hsql::SelectStatement* selectStatement);
string parseExpressionWithOperator(hsql::Expr* expr);
string parseExpressionWithoutOperator(hsql::Expr* expr);
string parseExpression(hsql::Expr* expr);

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
            std::string sql = execute(result, response);
            std::cout << sql << std::endl;
        }
        else if (response != QUIT) {
            std::cout << "Invalid SQL: " << response << std::endl;
        }

        delete [] responseArray;
        delete result;
    }
}

std::string execute(hsql::SQLParserResult* query, std::string response) {
    std::string finalQuery = "";
    int n = query->size();
    //hsql::StatementType statementType = query->getStatement(0)->type();

    // TODO: turn query variable into formal sql query
    for (int i = 0; i < n; i++) {
        const hsql::SQLStatement* statement = query->getStatement(i);
        //hsql::printStatementInfo(statement);

        hsql::StatementType statementType = statement->type();

        switch(statementType){
            case hsql::kStmtCreate: // create statement
                {

                    finalQuery += parseCreate(response);
                    break;

                    // finalQuery += "CREATE TABLE ";
                
                    //hsql::CreateStatement::CreateType type = hsql::CreateStatement::CreateType.kTable;
                    // hsql::CreateStatement *createStatement = new hsql::CreateStatement(hsql::CreateStatement::kTable); // assume create statements are always creating a table
                    
                    // std::string name = "students";
                    
                    // char* nameArray = new char[name.length() + 1];
                    // strcpy(nameArray, name.c_str());

                    // createStatement->tableName = nameArray;
                    // hsql::printCreateStatementInfo(createStatement, 0);
                    // //std::cout << "Table name: " << createStatement->tableName << std::endl;
                    // //std::cout << "File path: " << createStatement->filePath << std::endl;
                    // //std::cout << "columns: " << createStatement->columns << std::endl;

                    // delete createStatement;
                    // delete nameArray;
                }
            case hsql::kStmtInsert: // insert statement
            {
                finalQuery += "INSERT";
                break;
            }case hsql::kStmtSelect:{
                hsql::SelectStatement* selectStatement = (hsql::SelectStatement*)statement;
                cout << parseSelect(selectStatement);               
                break;
            }
        }
    }
    return finalQuery;
}

string parseTableRef(hsql::TableRef* tableRef){
    string finalString = "";

    if(tableRef->list != nullptr){ // if there is >1 table in the list, parse all of them
        for(auto it = tableRef->list->begin(); it != tableRef->list->end(); it++)
        finalString += parseTableRef(*it) + ", ";
    }

    hsql::TableRefType tableRefType = tableRef->type; // type of table expression (join, table name, etc)
    switch(tableRefType){
        case hsql::kTableName:{ // if the table ref is a table name
            finalString += tableRef->name; // add table name
            if(tableRef->alias != nullptr) // check if there's an alias (tableName AS alias)
                finalString += " AS " + (string)tableRef->alias; // add "AS alias"
            break;
        }
        case hsql::kTableJoin:{ // if the table ref is a join expression
            hsql::JoinDefinition* joinDefinition = tableRef->join;
            hsql::JoinType joinType = joinDefinition->type; // type of join (inner, outer, etc)
            hsql::Expr* joinCondition = joinDefinition->condition;
            string joinTypeString = ""; // type of join (inner, outer, etc)
            string leftTableString = parseTableRef(joinDefinition->left); // parse the left table in the join; get the string
            string rightTableString = parseTableRef(joinDefinition->right); // same for the right table in the join
                
            switch(joinType){
                case hsql::kJoinLeft:{
                    joinTypeString = "LEFT JOIN";
                    break;
                }case hsql::kJoinRight:{
                    joinTypeString = "RIGHT JOIN";
                    break;
                }case hsql::kJoinInner:{
                    joinTypeString = "JOIN";
                    break;
                }case hsql::kJoinOuter:{
                    joinTypeString = "OUTER JOIN";
                    break;
                }
            }
            finalString = leftTableString + " " + joinTypeString + " " + rightTableString + " ON " + parseExpression(joinCondition);
        }
    }

    return finalString;
}

// parse an expression, both with and without an operator
string parseExpression(hsql::Expr* expr){
    
    if(expr->exprList != nullptr){
        cout << endl << "EXPR LIST: ";
        for(auto it = expr->exprList->begin(); it != expr->exprList->end(); it++){
            cout << *it << "    ";
        }
        cout << endl;
    }
    if(expr->type == 7)
        return parseExpressionWithOperator(expr);

    return parseExpressionWithoutOperator(expr);
}

// parses expressions that do not contain an operator, i.e. literals and column refs
string parseExpressionWithoutOperator(hsql::Expr* expr){
    string finalString = "";

    switch(expr->type){
        // if it's a literal float, int, or string, just add that value to the final string
        case hsql::kExprLiteralFloat:
            cout << expr->fval;
            finalString += expr->fval; // TODO: bug
            break;
        case hsql::kExprLiteralInt:{
            cout << expr->ival;
            finalString += expr->ival; // TODO: bug
            break;
        }case hsql::kExprLiteralString:{
            finalString += expr->getName();
            break;
        }case hsql::kExprColumnRef:{ // a column name, might have a table name
            char* varName = expr->name;
            char* table = expr->table;

            if(table != nullptr) // if there's a table name, add tableName.colName to finalString
                finalString += (string)table + ".";
            finalString += varName;
            break;
        }

    }

    return finalString;
}

// parses expressions that contain operators, like WHERE or JOIN conditions, like a.x = b.x
// precondition: only call this for expressions that contain >=1 operator
string parseExpressionWithOperator(hsql::Expr* condition){
    string finalString = parseExpression(condition->expr); // parse the left hand side of the expression

    // add the operator character (=, >, etc.) to the final string, in between the LHS and RHS
    if(condition->opType != 0){ // if there is an operator, parse it
                switch(condition->opType){
                    case 3:{
                        switch(condition->opChar){
                            case '=':{
                                finalString += " = "; 
                                break;
                            }case '<':{
                                finalString += " < "; 
                                break;
                            }case '>':{
                                finalString += " > "; 
                                break;
                            }
                        }
                        break;
                    }case 4:{
                        finalString += " <> ";
                        break;
                    }case 5:{
                        finalString += " <= ";
                        break;
                    }case 6:{
                        finalString += " >= ";
                        break;
                    }
                }
    }

    return (finalString + parseExpression(condition->expr2)); // parse the RHS and add it
}

string parseSelect(hsql::SelectStatement* selectStatement){
    string finalString = "SELECT ";
    hsql::TableRef* table = selectStatement->fromTable;
    hsql::Expr* whereClause = selectStatement->whereClause;
    vector<hsql::Expr*>* columnsToSelect = selectStatement->selectList;

    for (hsql::Expr* expr : *selectStatement->selectList){
        hsql::ExprType exprType = expr->type; // type of expression

        switch(exprType){
            case hsql::kExprStar: // if we're selecting *, add * to the string
                finalString += "*";
                break;           

            // Bug? For some reason, if the expression type is star, the code will also enter this case,
            // but the expression name is null
            case hsql::kExprColumnRef:{ // if we are selecting columns, add the column names to the string
                if(expr->hasTable()) // if there's a table name before the column name (tableName.colName)
                    finalString += (string)expr->table + ".";
               
                if(expr->name != nullptr) finalString += (string)expr->name + ", ";

                if(expr->hasAlias()) finalString += (string)expr->alias + ", ";
                break;
            }
        }
    }

    finalString += (" FROM "+ parseTableRef(table));
    if(whereClause != nullptr)
        finalString += " WHERE " + parseExpression(whereClause);
    return finalString;
}

std::string parseCreate(std::string response) {
    std::stringstream ss(response);

    std::string parsed = "CREATE TABLE ";
    std::string temp;
    // get rid of create table
    ss >> temp >> temp;

    // name
    ss >> temp;

    parsed += temp + " ";

    bool isName = true;
    // get column names and types
    while (ss >> temp) {
        if (isName) {
            parsed += temp + " ";
        }
        else {
            // convert type to uppercase
            for (char &c : temp)
                c = std::toupper(c);
            if (temp == "INTEGER,")
                temp = "INT,";
            parsed += temp + " ";
        }
        isName = !isName;
    }

    return parsed;
}