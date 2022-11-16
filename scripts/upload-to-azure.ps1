# Get a container SAS URL from the Azure portal. It should look something like this:
#   https://mystorageaccount.blob.core.windows.net/mycontainer?sp=racw&st=2022-11-15T19:17:05Z&se=2022-11-16T03:17:05Z&spr=https&sv=2021-06-08&sr=c&sig=secretignature

Param(
    [Parameter(Mandatory=$true)][string] $OutputFolder,
    [Parameter(Mandatory=$true)][string] $ContainerSasUrl,
    [Parameter()][string] $SyncStateFile,
    [switch]$SkipDownload,
    [switch]$SkipUpload,
    [switch]$SkipUpdateSyncState
)

if (!(Get-Command azcopy -ErrorAction SilentlyContinue)) {
    Write-Warning "azcopy is not installed or available in the current PATH. Please install from https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10"
    Exit 1
}

$registrySeeder = ($PSScriptRoot + "/../src/Dan.EntityRegistrySeeder/bin/Release/net6.0/Dan.EntityRegistrySeeder.exe")

if (!(Test-Path -Path $registrySeeder)) {
    Write-Warning "Dan.EntityRegistrySeeder.exe not built, building ...";
    dotnet build --configuration Release ($PSScriptRoot + "/../src/Dan.EntityRegistrySeeder/Dan.EntityRegistrySeeder.csproj")

    if (!($?)) {
        Write-Warning "Build failed, exiting"
        Exit 1
    }
}

# Get date of yesterday, since the dump we're downloading may be up to 24h old. 
# Note the format, the ER API requires this exact form in UTC
$syncDate = (Get-Date).AddDays(-1)
$syncDateString = $syncdate.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")

if (!($SkipDownload)) {
    Write-Information "--- DOWNLOAD ---"
    . $registrySeeder --output_path=$OutputFolder --keep_downloaded --use_downloaded
}

if (!($SkipUpload)) {
    Write-Information "--- UPLOAD ---"
    $env:AZCOPY_CONCURRENCY_VALUE=16
    azcopy copy (Join-Path -Path $OutputFolder -ChildPath "*") $ContainerSasUrl --recursive --check-length=false --content-type='application/json'   
}

if (!($SkipUpdateSyncState)) {
    Write-Information "--- UPDATE SYNC STATE ---"

    $syncStateFileEnheter = Join-Path -Path $OutputFolder -ChildPath "state-enheter.json" 
    $syncStateFileUnderEnheter = Join-Path -Path $OutputFolder -ChildPath "state-underenheter.json"
    $syncStateFileAll = Join-Path -Path $OutputFolder -ChildPath "state-*.json"

    if ($SyncStateFile) {
        if (Test-Path -Path $SyncStateFile) {
            Copy-Item $SyncStateFile $syncStateFileEnheter
            Copy-Item $SyncStateFile $syncStateFileUnderEnheter
        }
        else {
            Write-Warning "Unable to find \"$SyncStateFile\", exiting"
            Exit 1
        }
    } 
    else {
        $syncState =
@"
{
    "LastUpdated": "$syncDateString"
}
"@
    $syncState | Out-File -FilePath $syncStateFileEnheter -Encoding ascii
    $syncState | Out-File -FilePath $syncStateFileUnderEnheter -Encoding ascii
        
    }
       
    azcopy copy $syncStateFileAll $ContainerSasUrl --content-type='application/json'
}
