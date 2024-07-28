using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoaderLibrary.Models
{
    public class ReferenceTables
    {
        public List<ReferenceTable> RefTables { get; }
        public ReferenceTables()
        {
            this.RefTables = new List<ReferenceTable>()
            {
                new ReferenceTable()
                { KeyAttribute = "BUSINESS_ASSOCIATE_ID", Table = "BUSINESS_ASSOCIATE", ValueAttribute= "BA_LONG_NAME"},
                new ReferenceTable()
                { KeyAttribute = "FIELD_ID", Table = "FIELD", ValueAttribute= "FIELD_NAME"},
                new ReferenceTable()
                { KeyAttribute = "WELL_DATUM_TYPE", Table = "R_WELL_DATUM_TYPE", ValueAttribute= "LONG_NAME"},
                new ReferenceTable()
                { KeyAttribute = "STATUS", Table = "R_WELL_STATUS", ValueAttribute= "LONG_NAME"},
            };
        }
    }
}
