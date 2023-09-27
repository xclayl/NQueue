namespace NQueue.Internal.Db.Postgres.DbMigrations
{
    internal class PostgresSchemaInfo
    {
        public PostgresSchemaInfo(string type, string name)
        {
            Type = type;
            Name = name;
        }

        public string Type { get; }
        public string Name { get; }
    }
}