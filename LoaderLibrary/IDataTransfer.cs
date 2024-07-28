using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoaderLibrary
{
    public interface IDataTransfer
    {
        Task Transferdata(string path, string connectionString);
    }
}
