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

        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            // IMPORTANT NOTE!
            // To properly secure secrets for Docker containers in production, mount a secret volumne
            // https://docs.microsoft.com/en-us/azure/container-instances/container-instances-volume-secret
            // The following is just for demo purposes!

            // Setup EventHub
            string EhConnectionString = Environment.GetEnvironmentVariable("EVENTHUB_CONNECTION_STRING");
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EhConnectionString);
            _eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            // Setup storage account
            string storageConnectionString = Environment.GetEnvironmentVariable("STORAGE_CONTAINER_STRING");
            string storageContainer = Environment.GetEnvironmentVariable("STORAGE_CONTAINER");
            string filePath = Environment.GetEnvironmentVariable("RATINGS_FILEPATH");

            while (true)
            {
                Console.WriteLine("Start sending data to Event Hubs");
                try
                {
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
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"An error occurred: {ex.ToString()}");
                }

                // Delay
                Console.WriteLine("Sleeping for one minute...");
                await Task.Delay(60000); //One minute
            }
            
            // Cleanup / Dev purposes
            //await _eventHubClient.CloseAsync();
            //Console.WriteLine("Press ENTER to exit.");
            //Console.ReadLine();
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
                    Timestamp = nowUnixTimestamp() 
                };
                return userRating;
            }
            catch(Exception ex)
            {
                Console.WriteLine($"An error occurred parsing UserRating data: {ex.ToString()}");
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
