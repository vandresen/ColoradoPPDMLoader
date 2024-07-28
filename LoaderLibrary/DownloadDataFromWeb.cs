using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.Support.UI;
using System.IO.Compression;

namespace LoaderLibrary
{
    public class DownloadDataFromWeb
    {
        private readonly string _path;
        private readonly string wellBoreUrl = @"https://ecmc.state.co.us/data2.html#/downloads";
        //private readonly string fileNameInZip = "Wells.dbf";

        public DownloadDataFromWeb(string path = @"C:\temp")
        {
            _path = path;
        }

        public void DownloadWells()
        {
            string url = wellBoreUrl;
            string file = "WELLS_SHP.ZIP";
            string filePath = _path + "/WELLS_SHP.ZIP";
            ChromeDownload(url, file);
        }

        private void ChromeDownload(string url, string file)
        {
            string filePath = _path + @"\" + file;
            bool getNewCache = SaveCache(filePath);

            ChromeOptions chromeOptions = new ChromeOptions();
            chromeOptions.AddArgument("--headless=new");
            chromeOptions.AddUserProfilePreference("download.prompt_for_download", false);
            chromeOptions.AddUserProfilePreference("download.default_directory", _path);
            chromeOptions.AddUserProfilePreference("disable-popup-blocking", "true");
            chromeOptions.AddArgument("--ignore-ssl-errors");
            chromeOptions.AddArgument("--ignore-certificate-errors");

            IWebDriver driver = new ChromeDriver(chromeOptions);

            try
            {
                if (getNewCache)
                {
                    driver.Navigate().GoToUrl(wellBoreUrl);
                    WebDriverWait wait = new WebDriverWait(driver, TimeSpan.FromSeconds(40));

                    IWebElement expandLink = wait.Until(drv =>
                    {
                        var element = drv.FindElement(By.LinkText("Well Surface Location Data (Updated Daily)"));
                        return (element != null && element.Displayed && element.Enabled) ? element : null;
                    });
                    expandLink.Click();

                    IWebElement downloadLink = wait.Until(drv =>
                    {
                        var element = drv.FindElement(By.LinkText("Well Spots (APIs)(10 Mb)"));
                        return (element != null && element.Displayed && element.Enabled) ? element : null;
                    });
                    downloadLink.Click();

                    WaitForFileDownload(_path, file, 60);
                }

                //string zipPath = _path + @"\" + file;
            }
            catch (NoSuchElementException e)
            {
                Exception error = new Exception(
                    $"Element not found: {e.Message}, " +
                    $"Page Source: {driver.PageSource}"
                    );
                throw error;
            }
            catch (Exception e)
            {
                Exception error = new Exception(
                    $"An error occurred: {e.Message}"
                    );
                throw error;
            }
            finally
            {
                driver.Quit();
            }
        }

        private void WaitForFileDownload(string downloadDirectory, string fileName, int timeoutInSeconds)
        {
            var filePath = Path.Combine(downloadDirectory, fileName);
            var endTime = DateTime.Now.AddSeconds(timeoutInSeconds);

            while (DateTime.Now < endTime)
            {
                if (File.Exists(filePath))
                {
                    Console.WriteLine("File downloaded successfully.");
                    return;
                }
                System.Threading.Thread.Sleep(1000);
            }

            throw new TimeoutException("File download timed out.");
        }

        private bool SaveCache(string filePath)
        {
            if (File.Exists(filePath))
            {
                DateTime lastWriteTime = File.GetLastWriteTime(filePath);
                DateTime currentDate = DateTime.Now;
                if (currentDate < lastWriteTime.AddDays(14))
                {
                    Console.WriteLine("The wellbore file was last written less than 14 days ago.");
                    return false;
                }
                File.Delete(filePath);
            }
            return true;
        }

    }
}
