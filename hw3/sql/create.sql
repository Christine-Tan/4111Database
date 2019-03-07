create table ColumnDefinition
(
	schemaName varchar(50) default 'my_catalog' not null,
	tableName varchar(40) not null,
	columnName varchar(20) not null,
	columnType varchar(20) not null,
	notNull varchar(10) null,
	primary key (tableName, columnName)
)
;

create table IndexDefinition
(
	schemaName varchar(50) default 'my_catalog' not null,
	tableName varchar(40) not null,
	indexName varchar(100) not null,
	indexType varchar(20) not null,
	columnName varchar(20) not null,
	position int null,
	primary key (tableName, indexName, columnName),
	constraint IndexDefinition_ColumnDefinition_tableName_columnName_fk
		foreign key (tableName, columnName) references my_catalog.ColumnDefinition (tableName, columnName)
)
;

create index IndexDefinition_ColumnDefinition_tableName_columnName_fk
	on IndexDefinition (tableName, columnName)
;

create table TableDefinition
(
	schemaName varchar(50) default 'CSVCatalog' not null,
	tableName varchar(40) not null
		primary key,
	fileName varchar(100) not null
)
;

alter table ColumnDefinition
	add constraint ColumnDefinition_TableDefinition_tableName_fk
		foreign key (tableName) references my_catalog.TableDefinition (tableName)
;

alter table IndexDefinition
	add constraint IndexDefinition_TableDefinition_tableName_fk
		foreign key (tableName) references my_catalog.TableDefinition (tableName)
;

