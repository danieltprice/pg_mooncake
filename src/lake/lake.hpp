#pragma once

namespace duckdb {

using Oid = unsigned int;

class Lake {
public:
    static void CreateTable(const string &table_name, const string &path, const vector<string> &column_names,
                            const vector<string> &column_types);

    static void AddFile(Oid oid, const string &file_name, int64_t file_size);

    static void DeleteFile(Oid oid, const string &file_name);

    static void Commit();
};

} // namespace duckdb
