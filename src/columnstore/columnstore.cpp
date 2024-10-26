#include "columnstore/columnstore.hpp"
#include "columnstore_metadata.hpp"
#include "duckdb/common/file_system.hpp"
#include "lake/lake.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
namespace duckdb {

void Columnstore::CreateTable(Oid oid) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    string full_path = metadata.GenerateFullPath(oid);
    if (!full_path.empty() && !duckdb::FileSystem::IsRemoteFile(full_path)) {
        FileSystem::CreateLocal()->CreateDirectory(full_path);
    }
    vector<string> types;
    vector<string> names;
    string table_name;
    metadata.TableGetMetadata(oid, table_name, names, types);
    metadata.TablesInsert(oid, full_path);
    Lake::CreateTable(table_name, full_path, names, types);
}

void Columnstore::DropTable(Oid oid) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    metadata.DataFilesDelete(oid);
    metadata.TablesDelete(oid);
}

void Columnstore::TruncateTable(Oid oid) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    vector<ColumnstoreMetadata::FileInfo> files = metadata.DataFilesSearch(oid);
    metadata.DataFilesDelete(oid);
    for (auto file : files) {
        Lake::DeleteFile(oid, file.file_name);
    }
}

string Columnstore::GetTableInfo(Oid oid) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    return metadata.TablesSearch(oid);
}

string Columnstore::GetSecretForPath(const string &path) {
    if (!duckdb::FileSystem::IsRemoteFile(path)) {
        return "{}";
    }
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    return metadata.SecretGetDeltaFormat(path);
}

void DuckDBLoadSecrets(duckdb::ClientContext &context) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    auto secrets = metadata.SecretGetDuckDbFormat();
    for (auto secret : secrets) {
        pgduckdb::DuckDBQueryOrThrow(context, secret.c_str());
    }
}

} // namespace duckdb
