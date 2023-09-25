namespace NQueue.Internal.SqlServer.DbMigrations
{
    internal class SchemaInfo
    {   
        public SchemaInfo(string type, string name)
        {
            Type = type;
            Name = name;
        }

        public string Type { get; }
        public string Name { get; }
    }
}