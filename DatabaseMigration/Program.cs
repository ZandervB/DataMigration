using DatabaseMigration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using System.Data.SqlClient;

namespace DataMigration
{
    class Program
    {
        private const string SourceConnectionStringKey = "SourceDB";
        private const string DestinationConnectionStringKey = "DestinationDB";
        private const int BatchSize = 1000;

        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.File("app.log", rollingInterval: RollingInterval.Day)
                .CreateLogger();

            var serviceProvider = new ServiceCollection()
                .AddLogging(builder => builder.AddSerilog())
                .BuildServiceProvider();

            var logger = serviceProvider.GetRequiredService<ILogger<Program>>();

            List<TableInfo> sourceTables = new List<TableInfo>
            {
                new TableInfo { TableName = "dbo.Table1", PrimaryKeyColumn = "PKId" },
                new TableInfo { TableName = "dbo.Table2", PrimaryKeyColumn = "PKId" }
            };

            List<TableInfo> destinationTables = new List<TableInfo>
            {
                new TableInfo { TableName = "DestinationTable1", PrimaryKeyColumn = "PKId" },
                new TableInfo { TableName = "DestinationTable2", PrimaryKeyColumn = "PKId" }
            };

            try
            {
                var configuration = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json")
                    .Build();

                string sourceConnectionString = configuration.GetConnectionString(SourceConnectionStringKey) ?? string.Empty;
                string destinationConnectionString = configuration.GetConnectionString(DestinationConnectionStringKey) ?? string.Empty;

                using (SqlConnection sourceConnection = new SqlConnection(sourceConnectionString))
                using (SqlConnection destinationConnection = new SqlConnection(destinationConnectionString))
                {
                    sourceConnection.Open();
                    destinationConnection.Open();

                    foreach (var sourceTableInfo in sourceTables)
                    {
                        int offset = 0;
                        bool hasMoreData = true;

                        while (hasMoreData)
                        {
                            // Query the source database for data in batches using table object properties
                            string selectQuery = $"SELECT * FROM {sourceTableInfo.TableName} ORDER BY {sourceTableInfo.PrimaryKeyColumn} OFFSET {offset} ROWS FETCH NEXT {BatchSize} ROWS ONLY";

                            using (SqlCommand command = new SqlCommand(selectQuery, sourceConnection))
                            {
                                using (SqlDataReader reader = command.ExecuteReader())
                                {
                                    hasMoreData = reader.HasRows;

                                    foreach (var destinationTableInfo in destinationTables)
                                    {
                                        // Use bulk copy to insert data into the destination table using table object properties
                                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(destinationConnection))
                                        {
                                            bulkCopy.DestinationTableName = destinationTableInfo.TableName;
                                            bulkCopy.WriteToServer(reader);
                                        }
                                    }

                                    offset += BatchSize;
                                }
                            }
                        }

                        logger.LogInformation("Data migration for {0} completed successfully.", sourceTableInfo.TableName);
                    }

                    logger.LogInformation("All data migrations completed successfully.");
                }
            }
            catch (Exception ex)
            {
                logger.LogError("An error occurred: {0}", ex.Message);
            }
            finally
            {
                Console.ReadLine();
                Log.CloseAndFlush();
            }
        }

    }
}
