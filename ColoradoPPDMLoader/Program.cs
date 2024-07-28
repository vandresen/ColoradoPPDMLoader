using ColoradoPPDMLoader;
using LoaderLibrary;
using LoaderLibrary.Data;
using LoaderLibrary.DataAccess;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.CommandLine;
using System.CommandLine.Invocation;

var connectionStringOption = new Option<string>(
    "--connection-string",
    description: "The connection string for the database.")
{
    IsRequired = true
};
connectionStringOption.AddAlias("-c"); // Add shorthand alias for connection string

var cacheFolderOption = new Option<string>(
    "--cache-folder",
    description: "The folder path for web cache files.")
{
    IsRequired = false
};

cacheFolderOption.AddAlias("-f");

var rootCommand = new RootCommand
{
    connectionStringOption,
    cacheFolderOption
};

rootCommand.Description = "Application with database connection and cache folder options";

rootCommand.SetHandler(async (string connectionString, string cacheFolder) =>
{
    Console.WriteLine($"Database Connection String: {connectionString}");
    Console.WriteLine($"Cache Folder: {cacheFolder}");

    using IHost host = Host.CreateDefaultBuilder(args)
        .ConfigureServices(services =>
        {
            services.AddSingleton<IDataTransfer, DataTransfer>();
            services.AddSingleton<IDataAccess, DapperDataAccess>();
            services.AddSingleton<IWellData, WellData>();
            services.AddSingleton<App>();
        })
        .Build();
    var app = host.Services.GetService<App>();
    await app!.Run(cacheFolder, connectionString);

    Console.WriteLine("Press Enter to exit...");
    Console.ReadLine();

    await host.StopAsync();
    await host.WaitForShutdownAsync();

}, connectionStringOption, cacheFolderOption);

await rootCommand.InvokeAsync(args);
