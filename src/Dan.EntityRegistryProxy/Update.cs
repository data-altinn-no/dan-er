using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Net;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Dan.EntityRegistryProxy.Models;
using ICSharpCode.SharpZipLib.GZip;
using Microsoft.Azure.Functions.Worker.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Azure;

namespace Dan.EntityRegistryProxy;

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
        await PerformUpdate(req.Url.Query.Contains("forceupdate"));
        return req.CreateResponse(HttpStatusCode.OK);
    }

    [Function(nameof(Update))]
    public async Task RunTimerAsync([TimerTrigger("49 49 * * * *" /* run every hour at xx:49:49 */)] TimerInfo myTimer)
    {
        await PerformUpdate();
    }

    private async Task PerformUpdate(bool forceUpdate = false)
    {
        var blobServiceClient = new BlobServiceClient(_configuration["AzureWebJobsStorage"]);
        var containerClient = blobServiceClient.GetBlobContainerClient(ContainerName);
        var stateBlobClient = containerClient.GetBlobClient(StateBlob);

        var fullUpdate = true;
        var syncState = new SyncState();

        if (!await containerClient.ExistsAsync())
        {
            await containerClient.CreateAsync();
        }
        else
        {
            try
            {
                var downloadResult = await stateBlobClient.DownloadContentAsync();
                var downloadedData = downloadResult.Value.Content.ToString();
                syncState = JsonConvert.DeserializeObject<SyncState>(downloadedData) ?? new SyncState();
                if (syncState.LastUpdatedSubUnits != DateTimeOffset.MinValue && syncState.LastUpdatedUnits != DateTimeOffset.MinValue)
                {
                    fullUpdate = false;
                }
            }
            catch (Exception)
            {
                _logger.LogWarning("Invalid or missing sync state, attempting full sync");
            }
        }

        if (fullUpdate || forceUpdate)
        {
            var updateUnitsTask = DownloadFromBrreg(containerClient, UnitType.Units);
            var updateSubUnitsTask = DownloadFromBrreg(containerClient, UnitType.SubUnits);

            await Task.WhenAll(updateUnitsTask, updateSubUnitsTask);
        }
        else
        {
            _logger.LogInformation("Syncing changes after " + syncState.LastUpdatedUnits.ToString("O"));
            // TODO! Implement fetching / sync of updates only
        }

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

    private async Task DownloadFromBrreg(BlobContainerClient containerClient, UnitType unitTypeEnum)
    {
        var unitType = unitTypeEnum == UnitType.Units ? "enheter" : "underenheter";

        _logger.LogInformation("Starting full sync of '" + unitType + "' ...");

        var sw = Stopwatch.StartNew();
        var client = _clientFactory.CreateClient(unitType);

        var response = await client.GetAsync("https://data.brreg.no/enhetsregisteret/oppslag/" + unitType + "/lastned",
            HttpCompletionOption.ResponseHeadersRead);

        _logger.LogInformation("Headers for '" + unitType + "' received, starting copy to temporary buffer ...");

        var contentStream = await response.Content.ReadAsStreamAsync();
        var inputBuffer = new MemoryStream();
        await contentStream.CopyToAsync(inputBuffer);
        inputBuffer.Seek(0, SeekOrigin.Begin);        

        _logger.LogInformation("Downloaded '" + unitType + "' to temporary buffer, starting upload ...");

        var cnt = 0;
        // Specify the StorageTransferOptions
        BlobUploadOptions options = new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = "application/json" },
            TransferOptions = new StorageTransferOptions
            {
                // Set the maximum number of workers that 
                // may be used in a parallel transfer.
                MaximumConcurrency = 64,

                // Set the maximum length of a transfer to 50MB.
                MaximumTransferSize = 50 * 1024 * 1024
            }
        };

        var tasks = new Queue<Task<Response<BlobContentInfo>>>();

        using (var gzipStream = new GZipInputStream(inputBuffer))
        using (var sr = new StreamReader(gzipStream))
        using (var reader = new JsonTextReader(sr))
        {
            while (reader.Read())
            {
                if (reader.TokenType == JsonToken.StartObject)
                {
                    var entry = JObject.Load(reader).ToObject<JObject>();
                    var blobClient = containerClient.GetBlobClient(unitType + "/" + entry!["organisasjonsnummer"]!);
                    tasks.Enqueue(blobClient.UploadAsync(BinaryData.FromString(entry.ToString(Formatting.None)), options));
                }
                cnt++;

#if DEBUG
                if (cnt % 100 == 0)
                {
                    _logger.LogInformation($"Written {cnt} '{unitType}' entries ({(float)cnt / sw.ElapsedMilliseconds * 1000} entries/sec)");
                }
#endif
            }
        }

        _logger.LogInformation("Awaiting remaining tasks (" +  tasks.Count + ") for '" + unitType + "'");
        await Task.WhenAll(tasks);

        var elapsed = sw.ElapsedMilliseconds;
        var persec = (float)cnt / elapsed * 1000;

        _logger.LogInformation($"Wrote {cnt} '{unitType}' entries in {elapsed / 1000} seconds ({persec} entries/sec)");
    }
}