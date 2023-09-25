namespace NQueue.Internal.SqlServer.DbMigrations
{
    internal class SqlServerSchemaInfo
    {   
        public SqlServerSchemaInfo(string type, string name)
        {
            Type = type;
            Name = name;
        }

        public string Type { get; }
        public string Name { get; }
    }
}