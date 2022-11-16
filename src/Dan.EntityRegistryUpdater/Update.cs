using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Functions.Worker.Http;
using Dan.EntityRegistryUpdater.Models;
using static Grpc.Core.Metadata;
using Microsoft.Extensions.Options;
using System.Xml;

namespace Dan.EntityRegistryUpdater;

public class Update
{
    private readonly IConfiguration _configuration;
    private readonly IHttpClientFactory _clientFactory;
    private readonly ILogger _logger;
    private readonly JsonSerializerOptions _options;

    private readonly List<UnitType> _unitTypes = new()
    {
        new UnitType
        {
            Name = "enheter", 
            StateBlob = "state-enheter.json",
            Url = "https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter/",
            IsSubUnit = false

        },
        new UnitType
        {
            Name = "underenheter",
            StateBlob = "state-underenheter.json",
            Url = "https://data.brreg.no/enhetsregisteret/api/oppdateringer/underenheter/",
            IsSubUnit = true
        },
    };

    private readonly BlobUploadOptions _blobUploadOptions = new()
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

    private const string ContainerName = "ccr";
    private const string DateFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";

    public Update(ILoggerFactory loggerFactory, IConfiguration configuration, IHttpClientFactory clientFactory)
    {
        _configuration = configuration;
        _clientFactory = clientFactory;
        _logger = loggerFactory.CreateLogger<Update>();

        _options = new JsonSerializerOptions() { WriteIndented = true };
        _options.Converters.Add(new CustomDateTimeOffsetConverter(DateFormat));
    }


    [Function("ManualUpdate")]
    public async Task<HttpResponseData> RunHttpAsync([HttpTrigger(AuthorizationLevel.Function)] HttpRequestData req)
    {
        await PerformUpdate();
        return req.CreateResponse(HttpStatusCode.OK);
    }

    [Function(nameof(Update))]
    public async Task RunTimerAsync([TimerTrigger("0 4,19,34,49 * * * *" /* run every 15 minutes starting av 4 minutes past */)] TimerInfo myTimer)
    {
       // await PerformUpdate();
    }

    private async Task PerformUpdate()
    {
        var blobServiceClient = new BlobServiceClient(_configuration["ErStorageConnectionString"]);
        var containerClient = blobServiceClient.GetBlobContainerClient(ContainerName);

        if (!await containerClient.ExistsAsync())
        {
            await containerClient.CreateAsync();
            _logger.LogInformation("Created container. Now run a seeding with Dan.EntityRegistrySeeder");
            return;
        }

        if (!int.TryParse(_configuration["PageSize"], out var pageSize))
        {
            pageSize = 30;
        }

        await Parallel.ForEachAsync(_unitTypes, async (unitType, cancellationToken) =>
        {
            SyncState syncState;

            try
            {
                syncState = await GetSyncState(containerClient, unitType.StateBlob);
            }
            catch (Exception)
            {
                _logger.LogWarning($"Invalid or missing sync state in {unitType.StateBlob}");
                return;
            }


            var url = unitType.Url + "?dato=" + syncState.LastUpdated.ToString(DateFormat) + "&size=" + pageSize;
            var httpClient = _clientFactory.CreateClient(unitType.Name);

            do
            {
                _logger.LogInformation("Fetching change list from " + url);
                var changeList =
                    (await httpClient.GetFromJsonAsync<ChangeList>(url, cancellationToken: cancellationToken))!;

                _logger.LogInformation("At page " + changeList.Page.Number + "/" + changeList.Page.TotalPages + " for '" + unitType.Name + "', iterating ");

                var taskList = new Queue<Task>();
                var changedUnits = unitType.IsSubUnit
                    ? changeList.Embedded.OppdaterteUnderenheter
                    : changeList.Embedded.OppdaterteEnheter;

                foreach (var changedUnit in changedUnits)
                {
                    taskList.Enqueue(SyncUnit(containerClient, changedUnit, unitType, cancellationToken));
                }

                await Task.WhenAll(taskList);

                if (changedUnits.Count > 0)
                {
                    syncState.LastUpdated = changedUnits.Last().Dato;
                    await SaveSyncState(containerClient, unitType.StateBlob, syncState);
                }

                // Handle exceeding offset, (page+1) * size, exceeding 10_000
                if ((changeList.Page.Number + 1) * changeList.Page.Size > 10_000)
                {
                    _logger.LogInformation("Next page exceeds 10_000 offset, bumping date");
                    url = unitType.Url + "?dato=" + syncState.LastUpdated.ToString(DateFormat) + "&size=" + pageSize;
                }
                else
                {
                    url = changeList.Links.Next?.Href.ToString();
                }
            } while (url != null);


        });

        _logger.LogInformation("All done!");
    }

    private async Task SyncUnit(BlobContainerClient containerClient, OppdatertEnhet changedUnit, UnitType unitType, CancellationToken cancellationToken)
    {
        var httpClient = _clientFactory.CreateClient(unitType.Name);
        var url = unitType.IsSubUnit ? changedUnit.Links.Underenhet.Href : changedUnit.Links.Enhet.Href;
        _logger.LogInformation("Syncing " + url);
        var response = await httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, url), cancellationToken);
        var blobClient = containerClient.GetBlobClient(unitType.Name + "/" + changedUnit.Organisasjonsnummer);
        if (response.StatusCode == HttpStatusCode.OK)
        {
            var json = await response.Content.ReadAsStringAsync(cancellationToken);
            await blobClient.UploadAsync(BinaryData.FromString(json), _blobUploadOptions, cancellationToken);
        }
        else
        {
            await blobClient.DeleteIfExistsAsync(cancellationToken: cancellationToken);
        }
        
    }

    private async Task<SyncState> GetSyncState(BlobContainerClient containerClient, string syncStateBlob)
    {
        var stateBlobClient = containerClient.GetBlobClient(syncStateBlob);
        var downloadResult = await stateBlobClient.DownloadContentAsync();
        var downloadedData = downloadResult.Value.Content.ToString();
        var syncState = JsonSerializer.Deserialize<SyncState>(downloadedData, _options) ?? new SyncState();
        if (syncState.LastUpdated == DateTimeOffset.MinValue)
        {
            throw new InvalidDataException();
        }

        return syncState;
    }

    private async Task SaveSyncState(BlobContainerClient containerClient, string syncStateBlob, SyncState syncState)
    {
        _logger.LogInformation("Saving sync state to " + syncStateBlob + ", last updated: " + syncState.LastUpdated.ToString(DateFormat) );
        var stateBlobClient = containerClient.GetBlobClient(syncStateBlob);
        syncState.LastUpdated = syncState.LastUpdated == DateTimeOffset.MinValue ? DateTimeOffset.UtcNow : syncState.LastUpdated;
        var updatedSyncStateJson = JsonSerializer.Serialize(syncState, _options);
        await stateBlobClient.UploadAsync(BinaryData.FromString(updatedSyncStateJson), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = "application/json" }
        });
    }
    
}