using System.Collections.Generic;
using System.IO;
using System.Text;

namespace NQueue.Internal.Db.SqlServer.DbMigrations
{
    internal abstract class SqlServerAbstractDbUpgrader
    {

        /// <summary>
        /// This method requires each "GO" to be the only characters on the line.  
        /// </summary>
        protected IEnumerable<string> SplitIntoBatches(string sql)
        {
            var batch = new StringBuilder();
            using var reader = new StringReader(sql);

            string? line;
            while ((line = reader.ReadLine()) != null)
            {
                if (line == "GO")
                {
                    if (batch.Length > 0)
                    {
                        yield return batch.ToString();
                        batch.Clear();
                    }
                }
                else
                {
                    batch.AppendLine(line);
                }
            }
            
            if (batch.Length > 0)
            {
                yield return batch.ToString();
                batch.Clear();
            }
        }
    }
}