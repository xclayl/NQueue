using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace NQueue.Internal.DbMigrations
{
    internal abstract class AbstractDbUpgrader
    {
        public abstract ValueTask Upgrade(string cnn);


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