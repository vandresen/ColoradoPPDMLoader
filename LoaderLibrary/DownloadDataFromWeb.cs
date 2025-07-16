using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.Support.UI;

namespace LoaderLibrary
{
    public class DownloadDataFromWeb
    {
        private readonly string _path;

        public DownloadDataFromWeb(string path = @"C:\temp")
        {
            _path = path;
        }

        //public string DownloadWells(string url)
        //{
        //    string downloadUrl = ChromeDownload(url);
        //    return downloadUrl;
        //}

        //private string ChromeDownload(string url)
        //{
        //    string directUrl = "";

        //    ChromeOptions chromeOptions = new ChromeOptions();
        //    chromeOptions.AddArgument("--headless=new");
        //    chromeOptions.AddUserProfilePreference("download.prompt_for_download", false);
        //    chromeOptions.AddUserProfilePreference("disable-popup-blocking", "true");
        //    chromeOptions.AddArgument("--ignore-ssl-errors");
        //    chromeOptions.AddArgument("--ignore-certificate-errors");

        //    IWebDriver driver = new ChromeDriver(chromeOptions);

        //    try
        //    {
                
        //            driver.Navigate().GoToUrl(url);
        //            WebDriverWait wait = new WebDriverWait(driver, TimeSpan.FromSeconds(40));

        //            IWebElement expandLink = wait.Until(drv =>
        //            {
        //                var element = drv.FindElement(By.LinkText("Well Surface Location Data (Updated Daily)"));
        //                return (element != null && element.Displayed && element.Enabled) ? element : null;
        //            });
        //            expandLink.Click();

        //            IWebElement downloadLink = wait.Until(drv =>
        //            {
        //                var element = drv.FindElement(By.LinkText("Well Spots (APIs)(10 Mb)"));
        //                return (element != null && element.Displayed && element.Enabled) ? element : null;
        //            });

        //            directUrl = downloadLink.GetAttribute("href");
        //    }
        //    catch (NoSuchElementException e)
        //    {
        //        Exception error = new Exception(
        //            $"Element not found: {e.Message}, " +
        //            $"Page Source: {driver.PageSource}"
        //            );
        //        throw error;
        //    }
        //    catch (Exception e)
        //    {
        //        Exception error = new Exception(
        //            $"An error occurred: {e.Message}"
        //            );
        //        throw error;
        //    }
        //    finally
        //    {
        //        driver.Quit();
        //    }

        //    return directUrl;
        //}

    }
}
