using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoaderLibrary.Models
{
    public class TableSchema
    {
        public string TABLE_NAME { get; set; }
        public string COLUMN_NAME { get; set; }
        public string DATA_TYPE { get; set; }
        public string TYPE_NAME { get; set; }
        public int LENGTH { get; set; }
        public int PRECISION { get; set; }
        public string SCALE { get; set; }
    }
}
