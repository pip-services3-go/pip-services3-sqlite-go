# Sqlite components for Pip.Services in Go Changelog

## <a name="1.2.0"></a> 1.2.0 (2021-04-03) 

### Features
* Moved SqliteConnection to connect package
* Added ISqlitePersistenceOverride interface to overload virtual methods

## <a name="1.1.0"></a> 1.1.0 (2021-02-19) 

### Features
* Renamed autoCreateObject to ensureSchema
* Added defineSchema method that shall be overriden in child classes
* Added clearSchema method

### Breaking changes
* Method autoCreateObject is deprecated and shall be renamed to ensureSchema


## <a name="1.0.0"></a> 1.0.0 (2020-12-16) 

### Features
* Implement SQLiteConnectionResolver
* Implement DefaultSqliteFactory
* Implement SQLiteConnection
* Implement SqlitePersistence
* Implement IdentifiableSqlitePersistence
* Implement IdentifiableJsonSqlitePersistence
