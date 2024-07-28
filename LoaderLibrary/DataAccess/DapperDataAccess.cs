using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoaderLibrary.DataAccess
{
    public class DapperDataAccess : IDataAccess
    {
        public async Task<IEnumerable<T>> LoadData<T, U>(string storedProcedure, U parameters, string connectionString)
        {
            using IDbConnection cnn = new SqlConnection(connectionString);
            return await cnn.QueryAsync<T>(storedProcedure, parameters, commandType: CommandType.StoredProcedure);
        }

        public Task<IEnumerable<T>> ReadData<T>(string sql, string connectionString)
        {
            throw new NotImplementedException();
        }

        public async Task SaveData<T>(string connectionString, T data, string sql)
        {
            using IDbConnection cnn = new SqlConnection(connectionString);
            await cnn.ExecuteAsync(sql, data);
        }
    }
}
