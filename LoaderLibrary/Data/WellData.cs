using LoaderLibrary.DataAccess;
using LoaderLibrary.Extensions;
using LoaderLibrary.Models;
using Microsoft.Extensions.Logging;
using NetTopologySuite.IO.Esri.Dbf;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.Support.UI;
using System;
using System.IO.Compression;

namespace LoaderLibrary.Data
{
    public class WellData : IWellData
    {
        private readonly string wellBoreUrl = @"https://ecmc.state.co.us/data2.html#/downloads";
        private readonly string file = "WELLS_SHP.ZIP";
        private readonly string fileNameInZip = "Wells.dbf";
        private readonly string bottomHoleFileInZip = "Directional_Bottomhole_Locations.dbf";
        private readonly ILogger _log;
        private readonly IDataAccess _da;

        public WellData(IDataAccess da, ILogger<WellData> log)
        {
            _log = log;
            _da = da;
        }

        public async Task CopyWellbores(string connectionString, string path)
        {
            // Retrieve column lengths for truncation
            var tableAttributes = await GetColumnInfo(connectionString, "WELL");
            int GetColumnLength(string name) => tableAttributes.FirstOrDefault(x => x.COLUMN_NAME == name)?.PRECISION ?? 4;

            int operatorLength = GetColumnLength("OPERATOR");
            int fieldLength = GetColumnLength("ASSIGNED_FIELD");
            int wellNameLength = GetColumnLength("WELL_NAME");

            var headers = new List<WellHeader>();
            using var client = new HttpClient();

            // === Download and read surface location data ===
            string surfaceUrl = GetDownloadLink(
                "Well Surface Location Data (Updated Daily)",
                "Well Spots (APIs)(10 Mb)"
            );

            string remark = $"Downloaded from {surfaceUrl}";
            using var surfaceDbf = await ReadDbfFromZipAsync(client, surfaceUrl, fileNameInZip);

            foreach (var record in surfaceDbf)
            {
                string api = "05" + (record["API"] as string ?? "");
                api = api + "00";
                string wellName = (record["Well_Name"] as string ?? "").Trim();
                string operatorName = (record["Operator"] as string ?? "").Trim();
                string fieldName = (record["Field_Name"] as string ?? "").Trim();

                var header = new WellHeader
                {
                    UWI = api,
                    WELL_NAME = wellName.Length > wellNameLength ? wellName[..wellNameLength] : wellName,
                    OPERATOR = operatorName.Length > operatorLength ? operatorName[..operatorLength] : operatorName,
                    ASSIGNED_FIELD = fieldName.Length > fieldLength ? fieldName[..fieldLength] : fieldName,
                    SPUD_DATE = record["Spud_Date"] as DateTime?,
                    DEPTH_DATUM_ELEV = record["Ground_Ele"] as int?,
                    DEPTH_DATUM = "GR",
                    FINAL_TD = record["Max_MD"] as int?,
                    CURRENT_STATUS_DATE = record["Stat_Date"] as DateTime?,
                    SURFACE_LONGITUDE = record["Longitude"] is double lon1 ? lon1 : (double?)null,
                    SURFACE_LATITUDE = record["Latitude"] is double lat1 ? lat1 : (double?)null,
                    CURRENT_STATUS = (record["Facil_Stat"] as string ?? "").Trim(),
                    REMARK = remark
                };

                headers.Add(header);
            }

            // === Optional: Download and merge bottom-hole locations ===
            string bottomUrl = GetDownloadLink(
                "Directional Well Data (Updated Daily)",
                "Directional Bottom Hole Locations (1 Mb)"
            );

            using var bottomDbf = await ReadDbfFromZipAsync(client, bottomUrl, bottomHoleFileInZip);

            foreach (var record in bottomDbf)
            {
                string api = "05" + (record["API"] as string ?? "");
                double? lat = record["Lat"] as double?;
                double? lon = record["Long"] as double?;
                double? td = record["MD"] as double?;
                string wellName = (record["Well_Name"] as string ?? "").Trim();
                string operatorName = (record["Operator"] as string ?? "").Trim();
                operatorName = operatorName.Length > operatorLength ? operatorName[..operatorLength] : operatorName;
                var match = headers.FirstOrDefault(h => h.UWI == api);
                if (match != null)
                {
                    match.BOTTOM_HOLE_LATITUDE = lat;
                    match.BOTTOM_HOLE_LONGITUDE = lon;
                    match.WELL_NAME = wellName;
                    match.OPERATOR = operatorName;
                    match.FINAL_TD = td;
                }
                else
                {
                    string uwi10 = api.Substring(0, 10);
                    var surfaceSource = headers.FirstOrDefault(h => h.UWI.StartsWith(uwi10));
                    if (surfaceSource != null)
                    {
                        // Create sidetrack copy
                        var sidetrack = new WellHeader
                        {
                            UWI = api, // keep it 10-digit to avoid duplicate with original
                            WELL_NAME = wellName,
                            OPERATOR = operatorName,
                            ASSIGNED_FIELD = surfaceSource.ASSIGNED_FIELD,
                            SPUD_DATE = surfaceSource.SPUD_DATE,
                            DEPTH_DATUM_ELEV = surfaceSource.DEPTH_DATUM_ELEV,
                            DEPTH_DATUM = surfaceSource.DEPTH_DATUM,
                            FINAL_TD = td,
                            CURRENT_STATUS_DATE = surfaceSource.CURRENT_STATUS_DATE,
                            SURFACE_LATITUDE = surfaceSource.SURFACE_LATITUDE,
                            SURFACE_LONGITUDE = surfaceSource.SURFACE_LONGITUDE,
                            CURRENT_STATUS = surfaceSource.CURRENT_STATUS,
                            REMARK = surfaceSource.REMARK + " (sidetrack inferred)",

                            BOTTOM_HOLE_LATITUDE = lat,
                            BOTTOM_HOLE_LONGITUDE = lon
                        };
                        headers.Add(sidetrack);
                    }
                }

            }

            
            await SaveWellbores(headers, connectionString);
        }


        private async Task<DbfReader> ReadDbfFromZipAsync(HttpClient client, string zipUrl, string entryName)
        {
            using var response = await client.GetAsync(zipUrl, HttpCompletionOption.ResponseHeadersRead);
            response.EnsureSuccessStatusCode();

            using var remoteZipStream = await response.Content.ReadAsStreamAsync();
            using var inputZip = new ZipArchive(remoteZipStream, ZipArchiveMode.Read, leaveOpen: false);

            var entry = inputZip.GetEntry(entryName)
                ?? throw new FileNotFoundException($"File '{entryName}' not found in downloaded zip.");

            var zipStream = new MemoryStream();
            using (var entryStream = entry.Open())
            {
                await entryStream.CopyToAsync(zipStream);
            }

            zipStream.Position = 0;
            return new DbfReader(zipStream); // assuming your `DbfReader` owns the stream
        }


        private async Task SaveWellbores(List<WellHeader> wellbores, string connectionString)
        {
            
            wellbores.Where(c => string.IsNullOrEmpty(c.OPERATOR)).Select(c => { c.OPERATOR = "UNKNOWN"; return c; }).ToList();
            wellbores.Where(c => string.IsNullOrEmpty(c.ASSIGNED_FIELD)).Select(c => { c.ASSIGNED_FIELD = "UNKNOWN"; return c; }).ToList();
            wellbores.Where(c => string.IsNullOrEmpty(c.DEPTH_DATUM)).Select(c => { c.DEPTH_DATUM = "UNKNOWN"; return c; }).ToList();
            wellbores.Where(c => string.IsNullOrEmpty(c.CURRENT_STATUS)).Select(c => { c.CURRENT_STATUS = "UNKNOWN"; return c; }).ToList();
            await SaveWellboreRefData(wellbores, connectionString);
            _log.LogInformation("Start saving wellbore data");
            string sql = "IF NOT EXISTS(SELECT 1 FROM WELL WHERE UWI = @UWI) " +
                "INSERT INTO WELL (UWI, WELL_NAME, OPERATOR, ASSIGNED_FIELD, SPUD_DATE, " +
                "DEPTH_DATUM_ELEV, DEPTH_DATUM, CURRENT_STATUS_DATE, CURRENT_STATUS,  " +
                "SURFACE_LONGITUDE, SURFACE_LATITUDE, REMARK) " +
                "VALUES(@UWI, @WELL_NAME, @OPERATOR, @ASSIGNED_FIELD, @SPUD_DATE, " +
                "@DEPTH_DATUM_ELEV, @DEPTH_DATUM, @CURRENT_STATUS_DATE, @CURRENT_STATUS,  " +
                "@SURFACE_LONGITUDE, @SURFACE_LATITUDE, @REMARK)";
            await _da.SaveData<IEnumerable<WellHeader>>(connectionString, wellbores, sql);
        }

        public async Task SaveWellboreRefData(List<WellHeader> wellbores, string connectionString)
        {
            _log.LogInformation("Start saving wellbore reference data");
            Dictionary<string, List<ReferenceData>> refDict = new Dictionary<string, List<ReferenceData>>();
            ReferenceTables tables = new ReferenceTables();

            List<ReferenceData> refs = wellbores.Select(x => x.OPERATOR).Distinct().ToList().CreateReferenceDataObject();
            refDict.Add(tables.RefTables[0].Table, refs);
            refs = wellbores.Select(x => x.ASSIGNED_FIELD).Distinct().ToList().CreateReferenceDataObject();
            refDict.Add(tables.RefTables[1].Table, refs);
            refs = wellbores.Select(x => x.DEPTH_DATUM).Distinct().ToList().CreateReferenceDataObject();
            refDict.Add(tables.RefTables[2].Table, refs);
            refs = wellbores.Select(x => x.CURRENT_STATUS).Distinct().ToList().CreateReferenceDataObject();
            refDict.Add(tables.RefTables[3].Table, refs);
            foreach (var table in tables.RefTables)
            {
                refs = refDict[table.Table];
                string sql = "";
                if (table.Table == "R_WELL_STATUS")
                {
                    sql = $"IF NOT EXISTS(SELECT 1 FROM {table.Table} WHERE {table.KeyAttribute} = @Reference) " +
                $"INSERT INTO {table.Table} " +
                $"(STATUS_TYPE, {table.KeyAttribute}, {table.ValueAttribute}) " +
                $"VALUES('STATUS', @Reference, @Reference)";
                }
                else
                {
                    sql = $"IF NOT EXISTS(SELECT 1 FROM {table.Table} WHERE {table.KeyAttribute} = @Reference) " +
                $"INSERT INTO {table.Table} " +
                $"({table.KeyAttribute}, {table.ValueAttribute}) " +
                $"VALUES(@Reference, @Reference)";
                }
                await _da.SaveData(connectionString, refs, sql);
            }
        }

        private string GetDownloadLink(string topLinkText, string nextLinkText)
        {
            ChromeOptions chromeOptions = new ChromeOptions();
            chromeOptions.AddArgument("--headless=new");
            chromeOptions.AddUserProfilePreference("download.prompt_for_download", false);
            chromeOptions.AddUserProfilePreference("disable-popup-blocking", "true");
            chromeOptions.AddArgument("--ignore-ssl-errors");
            chromeOptions.AddArgument("--ignore-certificate-errors");

            using (IWebDriver driver = new ChromeDriver(chromeOptions))
            {
                try
                {
                    driver.Navigate().GoToUrl(wellBoreUrl);
                    WebDriverWait wait = new WebDriverWait(driver, TimeSpan.FromSeconds(40));

                    IWebElement expandLink = wait.Until(drv =>
                    {
                        var element = drv.FindElement(By.LinkText(topLinkText));
                        return (element != null && element.Displayed && element.Enabled) ? element : null;
                    });
                    expandLink.Click();

                    IWebElement downloadLink = wait.Until(drv =>
                    {
                        var element = drv.FindElement(By.LinkText(nextLinkText));
                        return (element != null && element.Displayed && element.Enabled) ? element : null;
                    });

                    string directUrl = downloadLink.GetAttribute("href");

                    if (string.IsNullOrEmpty(directUrl))
                    {
                        throw new Exception("The download link did not contain an href attribute.");
                    }

                    Console.WriteLine($"Direct URL: {directUrl}");

                    return directUrl;
                }
                catch (NoSuchElementException e)
                {
                    throw new Exception(
                        $"Element not found: {e.Message}\nPage Source:\n{driver.PageSource}"
                    );
                }
                catch (Exception e)
                {
                    throw new Exception($"An error occurred: {e.Message}", e);
                }
            }
        }


        public Task<IEnumerable<TableSchema>> GetColumnInfo(string connectionString, string table) =>
            _da.LoadData<TableSchema, dynamic>("dbo.sp_columns", new { TABLE_NAME = table }, connectionString);
    }
}
