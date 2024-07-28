using LoaderLibrary.Models;

namespace LoaderLibrary.Data
{
    public interface IWellData
    {
        //Task<List<WellHeader>> ReadWellHeaderData(string connectionString);
        Task CopyWellbores(string connectionString, string path);
    }
}
