using LoaderLibrary.Data;
using LoaderLibrary.DataAccess;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoaderLibrary
{
    public class DataTransfer : IDataTransfer
    {
        private readonly IConfiguration _configuration;
        private readonly IDataAccess _da;
        private readonly IWellData _wellData;
        private readonly ILogger<DataTransfer> _log;

        public DataTransfer(ILogger<DataTransfer> log, IConfiguration configuration,
            IDataAccess da, IWellData wellData)
        {
            _log = log;
            _configuration = configuration;
            _da = da;
            _wellData = wellData;
        }

        public async Task Transferdata(string path, string connectionString)
        {
            try
            {
                _log.LogInformation("Start Data Transfer and Copy");
                await _wellData.CopyWellbores(connectionString, path);
                _log.LogInformation("Data has been Copied");
            }
            catch (Exception ex)
            {
                string errors = "Error transferring data: " + ex.ToString();
                _log.LogError(errors);
            }
        }
    }
}
