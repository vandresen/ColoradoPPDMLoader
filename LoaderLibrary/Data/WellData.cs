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
        private readonly ILogger _log;
        private readonly IDataAccess _da;

        public WellData(IDataAccess da, ILogger<WellData> log)
        {
            _log = log;
            _da = da;
        }

        public async Task CopyWellbores(string connectionString, string path)
        {
            IEnumerable<TableSchema> tableAttributeInfo = await GetColumnInfo(connectionString, "WELL");
            TableSchema? dataProperty = tableAttributeInfo.FirstOrDefault(x => x.COLUMN_NAME == "OPERATOR");
            int operatorLength = dataProperty == null ? 4 : dataProperty.PRECISION;
            dataProperty = tableAttributeInfo.FirstOrDefault(x => x.COLUMN_NAME == "ASSIGNED_FIELD");
            int fieldLength = dataProperty == null ? 4 : dataProperty.PRECISION;
            dataProperty = tableAttributeInfo.FirstOrDefault(x => x.COLUMN_NAME == "WELL_NAME");
            int wellNameLength = dataProperty == null ? 4 : dataProperty.PRECISION;

            List<WellHeader> headers = new List<WellHeader>();
            string downloadUrl = GetDownloadLink("Well Surface Location Data (Updated Daily)", "Well Spots (APIs)(10 Mb)");
            string remark = $"Downloaded from {downloadUrl}";

            using (HttpClient client = new HttpClient())
            using (HttpResponseMessage response = await client.GetAsync(downloadUrl, HttpCompletionOption.ResponseHeadersRead))
            {
                response.EnsureSuccessStatusCode();

                using (Stream remoteZipStream = await response.Content.ReadAsStreamAsync())
                using (ZipArchive inputZip = new ZipArchive(remoteZipStream, ZipArchiveMode.Read, leaveOpen: false))
                {
                    ZipArchiveEntry wellsEntry = inputZip.GetEntry(fileNameInZip);

                    if (wellsEntry == null)
                    {
                        throw new FileNotFoundException($"File '{fileNameInZip}' not found in downloaded zip.");
                    }
                    MemoryStream zipStream = new MemoryStream();
                    using (Stream stream = wellsEntry.Open())
                    {
                        stream.CopyTo(zipStream);
                    }

                    zipStream.Position = 0;

                    using var dbf = new DbfReader(zipStream);
                    foreach (var record in dbf)
                    {
                        string api = "05" + record["API"];
                        string apiCounty = (string)record["API_County"];
                        string apiSeq = (string)record["API_Seq"];
                        string apiLabel = (string)record["API_Label"];
                        int? operatorNum = (int?)record["Operat_Num"];
                        string operatorName = (string)record["Operator"];
                        string wellNum = (string)record["Well_Num"];
                        string wellName = (string)record["Well_Name"];
                        string wellTitle = (string)record["Well_Title"];
                        string citingType = (string)record["Citing_Typ"];
                        DateTime? spudDate = (DateTime?)record["Spud_Date"];
                        int? groundElev = (int?)record["Ground_Ele"];
                        int? maxMD = (int?)record["Max_MD"];
                        int? maxTVD = (int?)record["Max_TVD"];
                        int? fieldCode = (int?)record["Field_Code"];
                        string fieldName = (string)record["Field_Name"];
                        int? facilId = (int?)record["Facil_Id"];
                        string facilType = (string)record["Facil_Type"];
                        string facilStat = (string)record["Facil_Stat"];
                        string wellClass = (string)record["Well_Class"];
                        DateTime? statDate = (DateTime?)record["Stat_Date"];
                        string locQual = (string)record["Loc_Qual"];
                        int? locId = (int?)record["Loc_ID"];
                        string locName = (string)record["Loc_Name"];
                        int? distNS = (int?)record["Dist_N_S"];
                        string dirNS = (string)record["Dir_N_S"];
                        int? distEW = (int?)record["Dist_E_W"];
                        string dirEW = (string)record["Dir_E_W"];
                        string qtrQtr = (string)record["Qtr_Qtr"];
                        string section = (string)record["Section"];
                        string township = (string)record["Township"];
                        string range = (string)record["Range"];
                        string meridian = (string)record["Meridian"];
                        double latitude = (double)record["Latitude"];
                        double longitude = (double)record["Longitude"];
                        int? utmX = (int?)record["Utm_X"];
                        int? utmY = (int?)record["Utm_Y"];

                        WellHeader head = new WellHeader()
                        {
                            UWI = api,
                            WELL_NAME = wellName,
                            OPERATOR = operatorName,
                            ASSIGNED_FIELD = fieldName,
                            SPUD_DATE = spudDate,
                            DEPTH_DATUM_ELEV = groundElev,
                            DEPTH_DATUM = "GR",
                            FINAL_TD = maxMD,
                            CURRENT_STATUS_DATE = statDate,
                            SURFACE_LONGITUDE = longitude,
                            SURFACE_LATITUDE = latitude,
                            CURRENT_STATUS = facilStat,
                            REMARK = remark,
                        };

                        if (head.WELL_NAME.Length > wellNameLength)
                            head.WELL_NAME = head.WELL_NAME.Substring(0, wellNameLength);
                        if (head.OPERATOR.Length > operatorLength)
                            head.OPERATOR = head.OPERATOR.Substring(0, operatorLength);
                        if (head.ASSIGNED_FIELD.Length > fieldLength)
                            head.ASSIGNED_FIELD = head.ASSIGNED_FIELD.Substring(0, fieldLength);
                        headers.Add(head);
                    }
                }
            }


            await SaveWellbores(headers, connectionString);
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
