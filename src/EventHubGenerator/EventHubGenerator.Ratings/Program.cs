using System;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Microsoft.Azure.EventHubs;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.Collections.Generic;

namespace EventHubGenerator.Ratings
{
    class Program
    {
        private static EventHubClient _eventHubClient;
        private const string _ehEntityPath = "laceeh02";

        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            //// Setup EventHub
            //string EhConnectionString = Environment.GetEnvironmentVariable("EVENTHUB_CONNECTION_STRING");
            string EhConnectionString = "Endpoint=sb://laceeuseh01.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=TmBWRev8VoN9UtVP8OpogzWzT8HJJdkspLpBsFfZ8LQ=";
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EhConnectionString)
            {
                EntityPath = _ehEntityPath
            };
            _eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            // Setup storage account
            //string storageConnectionString = Environment.GetEnvironmentVariable("storageconnectionstring");
            //string storageContainer = Environment.GetEnvironmentVariable("storage_container");
            string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=laceeusstor01;AccountKey=HKx5gs8kTZVJLQc/et0pSLaX4OiGkRsh9a66LC9fObDlL/Jg4s/iEDsklV5aRucLrCQKOFXgwfD6xYIG/a4Lkg==;EndpointSuffix=core.windows.net";
            string storageContainer = "data";
            string filePath = "ml-latest-small/ratings.csv";

            foreach (var line in ReadBlobStorageFile(storageConnectionString, storageContainer, filePath))
            {
                var userRating = ToUserRating(line);
                if (userRating != null)
                {
                    var json = JsonConvert.SerializeObject(userRating);
                    Console.WriteLine(json);
                    await _eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(json)));
                }

                // Delay
                await Task.Delay(1000);
            }

            // Cleanup
            await _eventHubClient.CloseAsync();

            Console.WriteLine("Press ENTER to exit.");
            Console.ReadLine();
        }

        private static IEnumerable<string> ReadBlobStorageFile(string connectionString, string container, string filePath)
        {
            // Retrieve the connection string for use with the application. The storage connection string is stored
            // in an environment variable on the machine running the application called storageconnectionstring.
            // If the environment variable is created after the application is launched in a console or with Visual
            // Studio, the shell needs to be closed and reloaded to take the environment variable into account.
            CloudStorageAccount storageAccount;

            // Check whether the connection string can be parsed.
            if (CloudStorageAccount.TryParse(connectionString, out storageAccount))
            {
                // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer blobContainer = cloudBlobClient.GetContainerReference(container);

                // List the blobs in the container.
                CloudBlob blob = blobContainer.GetBlobReference(filePath);
                using (var stream = blob.OpenReadAsync())
                {
                    using (StreamReader reader = new StreamReader(stream.Result))
                    {
                        while (!reader.EndOfStream)
                        {
                            yield return reader.ReadLine();
                        }
                    }
                }
            }
            else
            {
                Console.WriteLine(
                    "A connection string has not been defined in the system environment variables. " +
                    "Add a environment variable named 'storageconnectionstring' with your storage " +
                    "connection string as a value.");
            }
        }

        private static UserRating ToUserRating(string str)
        {
            string[] strSplit = str.Split(",");
            try
            {
                var userRating = new UserRating()
                {
                    UserId = Convert.ToInt32(strSplit[0]),
                    MovieId = Convert.ToInt32(strSplit[1]),
                    Rating = Convert.ToDouble(strSplit[2]),
                    Timestamp = nowUnixTimestamp() //Convert.ToInt32(strSplit[3])
                };
                return userRating;
            }
            catch(Exception ex)
            {
                //TODO more robust
                return null;
            }
            
        }

        private static Int32 nowUnixTimestamp()
        {
            return (Int32)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
        }
    }

    public class UserRating
    {
        public int UserId { get; set; }
        public int MovieId { get; set; }
        public double Rating { get; set; }
        public Int32 Timestamp { get; set; }
    }
}
