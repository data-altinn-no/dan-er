using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Net;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using ICSharpCode.SharpZipLib.GZip;
using Microsoft.Azure.Functions.Worker.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Azure;
using Dan.EntityRegistryUpdater.Models;

namespace Dan.EntityRegistryUpdater;

public class Update
{
    private readonly IConfiguration _configuration;
    private readonly IHttpClientFactory _clientFactory;
    private readonly ILogger _logger;

    private const string ContainerName = "erproxy";
    private const string StateBlob = "state.json";

    public Update(ILoggerFactory loggerFactory, IConfiguration configuration, IHttpClientFactory clientFactory)
    {
        _configuration = configuration;
        _clientFactory = clientFactory;
        _logger = loggerFactory.CreateLogger<Update>();
    }


    [Function("ManualUpdate")]
    public async Task<HttpResponseData> RunHttpAsync([HttpTrigger(AuthorizationLevel.Function)] HttpRequestData req)
    {
        await PerformUpdate();
        return req.CreateResponse(HttpStatusCode.OK);
    }

    [Function(nameof(Update))]
    public async Task RunTimerAsync([TimerTrigger("49 49 * * * *" /* run every hour at xx:49:49 */)] TimerInfo myTimer)
    {
       // await PerformUpdate();
    }

    private async Task PerformUpdate()
    {
        var blobServiceClient = new BlobServiceClient(_configuration["RemoteStorage"]);
        var containerClient = blobServiceClient.GetBlobContainerClient(ContainerName);
        var stateBlobClient = containerClient.GetBlobClient(StateBlob);

        var canUpdate = false;
        var syncState = new SyncState();

        if (!await containerClient.ExistsAsync())
        {
            await containerClient.CreateAsync();
            _logger.LogInformation("Created container. Now run a seeding with Dan.EntityRegistrySeeder");
            return;
        }

        try
        {
            var downloadResult = await stateBlobClient.DownloadContentAsync();
            var downloadedData = downloadResult.Value.Content.ToString();
            syncState = JsonConvert.DeserializeObject<SyncState>(downloadedData) ?? new SyncState();
            if (syncState.LastUpdatedSubUnits == DateTimeOffset.MinValue || syncState.LastUpdatedUnits == DateTimeOffset.MinValue)
            {
                _logger.LogWarning($"Invalid sync state, {StateBlob} contains invalid dates.");
                return;
            }
        }
        catch (Exception)
        {
            _logger.LogWarning($"Invalid or missing sync state in {StateBlob}");
            return;
        }

        _logger.LogInformation("Syncing changes after " + syncState.LastUpdatedUnits.ToString("O"));
        
        // TODO! Implement fetching / sync of updates only

        _logger.LogInformation("Sync completed, updating sync state ...");

        syncState.LastUpdatedUnits = syncState.LastUpdatedUnits == DateTimeOffset.MinValue ? DateTimeOffset.UtcNow : syncState.LastUpdatedUnits;
        syncState.LastUpdatedSubUnits = syncState.LastUpdatedSubUnits == DateTimeOffset.MinValue ? DateTimeOffset.UtcNow : syncState.LastUpdatedSubUnits;

        var updatedSyncStateJson = JsonConvert.SerializeObject(syncState);
        await stateBlobClient.UploadAsync(BinaryData.FromString(updatedSyncStateJson), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = "application/json" }
        });

        _logger.LogInformation("All done!");
    }

    
}