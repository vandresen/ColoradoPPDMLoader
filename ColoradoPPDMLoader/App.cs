using LoaderLibrary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ColoradoPPDMLoader
{
    public class App
    {
        private readonly IDataTransfer _dataTransfer;

        public App(IDataTransfer dataTransfer)
        {
            _dataTransfer = dataTransfer;
        }

        public async Task Run(string path, string connectionString)
        {
            if (String.IsNullOrEmpty(path)) path = @"C:\temp";
            await _dataTransfer.Transferdata(path, connectionString);
        }
    }
}
