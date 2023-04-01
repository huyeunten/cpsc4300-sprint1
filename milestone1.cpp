#include "../sql-parser/src/SQLParser.h"
#include "../sql-parser/src/sqlhelper.h"
#include "db_cxx.h"
#include <iostream>
#include <cstring>

const std::string QUIT = "quit";

std::string execute(hsql::SQLParserResult* query);

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cout << "Usage: ./milestone1 /path" << std::endl;
        return -1;
    }

    std::string directory = argv[1];

    // maybe check if directory is valid?

    // TODO: create db in directory

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
    // TODO: turn query variable into formal sql query
    for (int i = 0; i < n; i++) {
        hsql::printStatementInfo(query->getStatement(i));
    }
    return finalQuery;
}