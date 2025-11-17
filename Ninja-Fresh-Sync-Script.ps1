<#
.SYNOPSIS
An enterprise-grade script to configure and synchronize NinjaOne devices as assets in Freshservice.

.DESCRIPTION
This script is designed for secure, automated, and scheduled execution. It uses an external config file,
supports the PowerShell SecretManagement module, performs efficient delta-syncing, and provides webhook notifications.

Key Features:
- External Configuration: All settings are managed in a `config.json` file for easy management.
- Secure Credential Handling: Automatically uses the PowerShell SecretManagement module if available.
- Delta Sync: Keeps a `state.json` file to only process devices updated since the last successful run.
- Webhook Notifications: Sends a summary on completion and an alert on failure to a Slack/Teams webhook.
- Resilient Processing: Errors on a single device will be logged without stopping the entire sync process.
- Auto-Product Creation: Automatically creates Freshservice Products for unmapped NinjaOne device models.
- Identifier Normalization: Cleans and standardizes unique identifiers to ensure high match rates.

.PARAMETER Mode
Specifies the operational mode.
'config' - Runs the interactive setup wizard.
'sync'   - Runs the automated synchronization process.

.PARAMETER ConfigFilePath
Path to the `config.json` file. Defaults to '.\config.json' in the script's directory.

.PARAMETER NinjaRegion
(Mandatory) The NinjaOne region shortcode (e.g., 'us', 'eu', 'oc').

.PARAMETER NinjaClientId
(Optional) Override the Client ID from the config or secret vault. For testing or manual runs.

.PARAMETER NinjaClientSecret
(Optional) Override the Client Secret from the config or secret vault. For testing or manual runs.

.PARAMETER FreshSubdomain
The subdomain of your Freshservice instance (e.g., 'your-company'). Can also be set via $env:FRESHSERVICE_SUBDOMAIN.

.PARAMETER FreshApiKey
(Optional) Override the Freshservice API Key from the config or secret vault. For testing or manual runs.

.PARAMETER DryRun
A switch that, if present in 'sync' mode, will log what actions it would have taken without making any actual changes in Freshservice.

.PARAMETER UpdateExisting
A switch that, if present in 'sync' mode, allows the script to update existing assets in Freshservice. By default, existing assets are skipped.

.EXAMPLE
# Run the interactive configuration wizard using settings from config.json
.\Ninja-Fresh-Sync-Script.ps1 -Mode config

.EXAMPLE
# Run the automated sync using settings from config.json.
# The script will handle credentials securely and only process changed devices.
.\Ninja-Fresh-Sync-Script.ps1 -Mode sync

.NOTES
Disclaimer: Human-authored, with assistance from code completion tools.
Version: 6.1
#>

[CmdletBinding()]
param(
    [ValidateSet("config","sync")]
    [string]$Mode            = "config",

    [string]$ConfigFilePath  = (Join-Path -Path $PSScriptRoot -ChildPath "config.json"),

    # Optional overrides for credentials
    [string]$NinjaClientId,
    [string]$NinjaClientSecret,
    [string]$FreshApiKey
)

# ==========================================================
# LOGGING & WEBHOOK SETUP
# ==========================================================
$LogDirectory = Join-Path -Path $PSScriptRoot -ChildPath "logs"
if (-not (Test-Path -Path $LogDirectory)) { New-Item -Path $LogDirectory -ItemType Directory | Out-Null }
$script:LogFile = Join-Path -Path $LogDirectory -ChildPath "sync-log-$(Get-Date -Format 'yyyy-MM-dd').log"

enum LogLevel { INFO, WARN, ERROR, DEBUG }

function Write-Log {
    [CmdletBinding()]
    param([string]$Message, [LogLevel]$Level = [LogLevel]::INFO)
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:s'
    $logLine = "[$timestamp] [$Level] $Message"
    Add-Content -Path $script:LogFile -Value $logLine
    $color = switch ($Level) {
        'INFO'  { "Cyan" } 'WARN'  { "Yellow" } 'ERROR' { "Red" } 'DEBUG' { "Gray" } default { "White" }
    }
    if ($Message -notmatch "\[\d+\]" -and $Message -notmatch "Select" -and $Message -notmatch "Enter") {
        Write-Host $logLine -ForegroundColor $color
    } else { Write-Host $Message }
}

function Send-WebhookNotification {
    param([string]$Title, [string]$Message, [string]$Color = "good")
    if ([string]::IsNullOrWhiteSpace($script:Config.WebhookUri)) { return }
    $payload = @{
        attachments = @( @{ title = $Title; text = $Message; color = $Color } )
    } | ConvertTo-Json
    try {
        Invoke-RestMethod -Uri $script:Config.WebhookUri -Method Post -Body $payload -ContentType 'application/json' -ErrorAction Stop
        Write-Log -Message "Successfully sent webhook notification." -Level DEBUG
    } catch {
        Write-Log -Message "Failed to send webhook notification: $($_.Exception.Message)" -Level WARN
    }
}

# ==========================================================
# CORE HELPERS
# ==========================================================
function Invoke-RobustApiCall {
    [CmdletBinding()]
    param([hashtable]$Params)
    $MaxRetries = 5; $InitialRetryDelay = 5
    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            Write-Log -Message "API call attempt $attempt to $($Params.Uri)" -Level DEBUG
            $Params["ErrorAction"] = "Stop"
            return Invoke-WebRequest @Params
        } catch {
            $statusCode = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { 0 }
            if ($statusCode -in 400,401,403,404) { Write-Log -Message "Non-retriable status code $statusCode." -Level ERROR; throw }
            if ($attempt -eq $MaxRetries) { Write-Log -Message "Final API attempt failed." -Level ERROR; throw }
            $backoffSeconds = 0
            if ($statusCode -eq 429 -and $_.Exception.Response.Headers['Retry-After']) {
                $backoffSeconds = [int]$_.Exception.Response.Headers['Retry-After']
                Write-Log -Message "API rate limit hit. Waiting for $backoffSeconds seconds (from Retry-After header)." -Level WARN
            } else {
                $backoffSeconds = [int]([Math]::Min($InitialRetryDelay * ([Math]::Pow(2, $attempt - 1)), 300))
                $backoffSeconds += Get-Random -Minimum 0 -Maximum ([Math]::Ceiling($backoffSeconds * 0.1))
                $logMsg = if ($statusCode -ne 0) { "API call failed with status $statusCode." } else { "API call failed with no HTTP response (e.g., network error)." }
                Write-Log -Message "$logMsg Retrying in $backoffSeconds seconds..." -Level WARN
            }
            Start-Sleep -Seconds $backoffSeconds
        }
    }
}

function Normalize-Identifier {
    param([string]$Identifier)
    if ([string]::IsNullOrWhiteSpace($Identifier)) { return $null }
    return $Identifier.Trim().ToLower() -replace '[-_\s]'
}

function Resolve-Credential {
    param([string]$ParamValue, [string]$SecretName, [string]$EnvVarName)
    if (-not [string]::IsNullOrWhiteSpace($ParamValue)) {
        Write-Log -Message "Using credential from command-line parameter." -Level DEBUG
        return $ParamValue
    }
    if ($script:Config.Secrets.UseSecretManagement -and $script:SecretManagementAvailable) {
        try {
            $secret = Get-Secret -Name $SecretName -AsPlainText -ErrorAction Stop
            Write-Log -Message "Successfully retrieved secret '$SecretName' from vault." -Level INFO
            return $secret
        } catch { Write-Log -Message "Secret '$SecretName' not found in vault. Checking environment variables." -Level DEBUG }
    }
    $envValue = $env:$EnvVarName
    if (-not [string]::IsNullOrWhiteSpace($envValue)) {
        Write-Log -Message "Using credential from environment variable '$EnvVarName'." -Level DEBUG
        return $envValue
    }
    return $null
}

# ---------------- API WRAPPERS & DATA FETCHERS ----------------
function Get-NinjaBaseUri { param([string]$Region) "https://$($Region.Trim()).ninjarmm.com" }
function Get-NinjaAccessToken {
    param([string]$BaseUri, [string]$ClientId, [string]$ClientSecret)
    Write-Log -Message "Requesting NinjaOne access token..."
    $body = "grant_type=client_credentials&client_id=$([System.Web.HttpUtility]::UrlEncode($ClientId))&client_secret=$([System.Web.HttpUtility]::UrlEncode($ClientSecret))&scope=monitoring management"
    $params = @{ Uri = "$BaseUri/ws/oauth/token"; Method = "POST"; Headers = @{ "Content-Type"="application/x-www-form-urlencoded";"Accept"="application/json" }; Body = $body }
    $tokenInfo = (Invoke-RobustApiCall -Params $params).Content | ConvertFrom-Json
    if (-not $tokenInfo.access_token) { throw "NinjaOne token response did not contain access_token." }
    Write-Log -Message "Successfully obtained NinjaOne access token."
    return $tokenInfo.access_token
}
function Invoke-ApiCall {
    param([string]$BaseUri, [string]$Method, [string]$Path, [hashtable]$Headers, [hashtable]$Query, [object]$Body)
    $uriBuilder = [System.UriBuilder]"$BaseUri$Path"
    if ($Query) {
        $uriBuilder.Query = ($Query.GetEnumerator() | ForEach-Object { "$([System.Web.HttpUtility]::UrlEncode($_.Key))=$([System.Web.HttpUtility]::UrlEncode([string]$_.Value))" }) -join "&"
    }
    $params = @{ Uri = $uriBuilder.Uri.AbsoluteUri; Method = $Method; Headers = $Headers }
    if ($Body) {
        $params["Body"] = ($Body | ConvertTo-Json -Depth 10)
        $params["ContentType"] = "application/json"
    }
    return Invoke-RobustApiCall -Params $params
}
function Invoke-NinjaApi {
    param([string]$Method, [string]$Path, [hashtable]$Query)
    $headers = @{ "Authorization" = "Bearer $($script:NinjaAccessToken)"; "Accept" = "application/json" }
    $resp = Invoke-ApiCall -BaseUri $script:NinjaBaseUri -Method $Method -Path $Path -Headers $headers -Query $Query
    return $resp.Content | ConvertFrom-Json
}
function Invoke-FreshApi {
    param([string]$Method, [string]$Path, [hashtable]$Query, [object]$Body)
    $headers = @{ "Authorization" = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("$($script:FreshApiKey):X")); "Accept" = "application/json" }
    $resp = Invoke-ApiCall -BaseUri "https://$($script:Config.FreshSubdomain).freshservice.com/api/v2" -Method $Method -Path $Path -Headers $headers -Query $Query -Body $Body
    return $resp.Content | ConvertFrom-Json
}
function Get-FreshAssetsExisting {
    Write-Log -Message "Fetching existing Freshservice assets..."
    $allAssets = @()
    $nextUrl = "https://$($script:Config.FreshSubdomain).freshservice.com/api/v2/assets?include=custom_fields&per_page=100"
    $headers = @{ "Authorization" = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("$($script:FreshApiKey):X")); "Accept" = "application/json" }
    while ($nextUrl) {
        $params = @{ Uri = $nextUrl; Headers = $headers; Method = "GET" }
        $resp = Invoke-RobustApiCall -Params $params
        $json = $resp.Content | ConvertFrom-Json
        if ($json.assets) {
            $allAssets += $json.assets
            Write-Log -Message "Fetched $($allAssets.Count) assets so far..." -Level DEBUG
        }
        $nextUrl = if ($resp.Headers["Link"]) { $resp.Headers["Link"] -match '<(.*?)>;\s*rel="next"' | Out-Null; $matches[1] } else { $null }
    }
    Write-Log -Message "Total Freshservice assets fetched: $($allAssets.Count)"
    return $allAssets
}
function Get-NinjaDevicesDetailed {
    Write-Log -Message "Fetching NinjaOne devices..."
    $allDevices = @()
    $after = $null
    do {
        $resp = Invoke-NinjaApi -Method GET -Path "/api/v2/devices-detailed" -Query @{ pageSize = 1000; after = $after }
        $batch = if ($resp -is [System.Collections.IEnumerable] -and -not $resp.PSObject.Properties.Match("items")) { @($resp) } `
                 elseif ($resp.items) { @($resp.items) } else { @($resp) }
        $allDevices += $batch
        $after = if ($batch.Count -eq 1000) { $batch[-1].id } else { $null }
        Write-Log -Message "Fetched $($allDevices.Count) devices so far..." -Level DEBUG
    } while ($after)
    Write-Log -Message "Total NinjaOne devices fetched: $($allDevices.Count)"
    return $allDevices
}
function Get-FreshProducts {
    Write-Log -Message "Loading Freshservice products..."
    $result = Invoke-FreshApi -Method GET -Path "/products" -Query @{ per_page = 100 }
    return if ($result.products) { $result.products } else { @() }
}
function Get-NinjaFieldValue {
    param([psobject]$Device, [string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path)) { return $null }
    $parts = $Path -split '\.'
    $current = $Device
    foreach ($part in $parts) {
        if ($null -eq $current -or $current.PSObject.Properties.Name -notcontains $part) { return $null }
        $current = $current.$part
    }
    return $current
}

# ---------------- CONFIG MODE FUNCTIONS -----------------
function Choose-FromList {
    param([object[]]$Items, [string]$DisplayProperty, [string]$Prompt = "Select item", [switch]$AllowSkip)
    if (-not $Items) { Write-Log -Message "No items available to choose from." -Level WARN; return $null }
    for ($i=0; $i -lt $Items.Count; $i++) {
        $label = if ([string]::IsNullOrWhiteSpace($Items[$i].$DisplayProperty)) { "(no $DisplayProperty)" } else { $Items[$i].$DisplayProperty }
        Write-Log -Message "[$i] $label"
    }
    if ($AllowSkip) { Write-Log -Message "[S] Skip" -Level WARN }
    while ($true) {
        $input = Read-Host "$Prompt (0..$($Items.Count-1)$(if ($AllowSkip){', or S to skip'} else {''}))"
        if ($AllowSkip -and $input -in 'S','s') { return $null }
        if ($input -as [int] -ge 0 -and $input -as [int] -lt $Items.Count) { return $Items[[int]$input] }
        Write-Log -Message "Invalid selection, try again." -Level WARN
    }
}
function Prompt-NinjaFieldForFsField {
    param([psobject]$FsField, [string[]]$Candidates)
    Write-Log -Message "`nFreshservice required field: $($FsField.label) (name: $($FsField.name))"
    for ($i=0;$i -lt $Candidates.Count;$i++) { Write-Log -Message "[$i] $($Candidates[$i])" }
    $input = Read-Host "Select NinjaOne field index, enter a custom path, or press Enter to skip"
    if ([string]::IsNullOrWhiteSpace($input)) { return $null }
    if ($input -as [int] -ge 0 -and $input -as [int] -lt $Candidates.Count) { return $Candidates[[int]$input] }
    return $input
}
function Configure-RoleMappings {
    param([object[]]$Devices, [object[]]$AssetTypes, [string[]]$NinjaFieldCandidates)
    Write-Log -Message "`nStep 1: Map NinjaOne nodeClass (role) to Freshservice asset types"
    $roles = $Devices.nodeClass | Where-Object {$_} | Sort-Object -Unique
    if (-not $roles) { Write-Log "No nodeClass values found on NinjaOne devices." -Level WARN; return @{} }
    $roleMap = @{}
    foreach ($role in $roles) {
        Write-Log -Message "`nRole: $role"
        $assetType = Choose-FromList -Items $AssetTypes -DisplayProperty "name" -Prompt "Select FS asset type for role '$role'" -AllowSkip
        if (-not $assetType) { Write-Log "Skipping role '$role'." -Level WARN; continue }
        $fieldGroups = Invoke-FreshApi -Method GET -Path "/asset_types/$($assetType.id)/fields"
        $allFields = $fieldGroups.asset_type_fields.fields | ForEach-Object {$_}
        $fieldMappings = @()
        foreach ($rf in ($allFields | Where-Object {$_.required})) {
            if ($rf.name -eq "asset_type_id") { continue }
            $ninjaPath = if ($rf.name -eq "name") { "systemName" } else { Prompt-NinjaFieldForFsField -FsField $rf -Candidates $NinjaFieldCandidates }
            if ($ninjaPath) { $fieldMappings += @{ fs_name=$rf.name; fs_label=$rf.label; ninja_field=$ninjaPath } }
        }
        $uniqueIdMapping = $null
        if ($fieldMappings.Count -gt 0) {
            Write-Log -Message "`nChoose the Unique Identifier for role '$role'."
            $choices = $fieldMappings | ForEach-Object {[pscustomobject]@{Display="FS:'$($_.fs_label)'<-Ninja:'$($_.ninja_field)'";Mapping=$_}}
            $chosen = Choose-FromList -Items $choices -DisplayProperty "Display" -Prompt "Select unique identifier"
            $uniqueIdMapping = if ($chosen) { $chosen.Mapping } else { $null }
            if ($uniqueIdMapping) { Write-Log "Unique ID for '$role' set to FS field '$($uniqueIdMapping.fs_name)'." }
        }
        $roleMap[$role] = @{ asset_type_id=$assetType.id; asset_type_name=$assetType.name; unique_identifier_mapping=$uniqueIdMapping; field_mappings=$fieldMappings }
    }
    return $roleMap
}
function Configure-ModelToProduct {
    param([object[]]$Devices, [object[]]$Products)
    Write-Log -Message "`nStep 2: Map NinjaOne system.model values to Freshservice products"
    $models = $Devices | Select-Object @{N='Mfr';E={$_.system.manufacturer}},@{N='Model';E={$_.system.model}} | Where-Object {$_.Model} | Group-Object Model,Mfr | ForEach-Object {$_.Group[0]}
    $modelMap = @{}
    $previewProducts = $Products | Select-Object -First 50
    if ($previewProducts) {
        Write-Log -Message "Preview of Freshservice products (first 50):"
        for ($i=0; $i -lt $previewProducts.Count; $i++) { Write-Log -Message "[$i] $($previewProducts[$i].name) (id $($previewProducts[$i].id))" }
    }
    foreach ($m in $models) {
        Write-Log -Message "`nModel: $($m.Model) (Manufacturer: $($m.Mfr))"
        $input = Read-Host "Enter Freshservice product id, (S)kip, (C)reate, or Enter to choose from list"
        if ($input -in 'S','s') { Write-Log "Skipping '$($m.Model)'." -Level WARN; continue }
        if ($input -in 'C','c') {
            $existing = $Products | Where-Object {$_.name -eq $m.Model}
            if ($existing) {
                Write-Log "Product '$($m.Model)' already exists. Using existing ID." -Level WARN
                $modelMap[$m.Model] = @{product_id=$existing.id;product_name=$existing.name}
            } else {
                try {
                    $newProd = Invoke-FreshApi -Method POST -Path "/products" -Body @{product=@{name=$m.Model;manufacturer=$m.Mfr}}
                    $modelMap[$m.Model] = @{product_id=$newProd.product.id;product_name=$newProd.product.name}
                    $Products += $newProd.product
                    Write-Log "Created new product '$($newProd.product.name)'."
                } catch { Write-Log "Failed to create product '$($m.Model)': $($_.Exception.Message)" -Level ERROR }
            }
        } elseif ([string]::IsNullOrWhiteSpace($input)) {
            $idx = Read-Host "Select preview index for product (0..$($previewProducts.Count-1), or S to skip)"
            if ($idx -in 'S','s') { Write-Log "Skipping model." -Level WARN; continue }
            if ($idx -as [int] -ge 0 -and $idx -as [int] -lt $previewProducts.Count) {
                $prod = $previewProducts[[int]$idx]
                $modelMap[$m.Model] = @{ product_id = $prod.id; product_name = $prod.name }
                Write-Log "Mapped model '$($m.Model)' -> product '$($prod.name)'."
            } else { Write-Log "Invalid index, skipping." -Level WARN }
        } elseif ($input -as [long]) {
            $prod = $Products | Where-Object { $_.id -eq [long]$input }
            if ($prod) {
                $modelMap[$m.Model] = @{ product_id = $prod.id; product_name = $prod.name }
                Write-Log "Mapped model '$($m.Model)' -> product '$($prod.name)'."
            } else { Write-Log "No product with id $input found, skipping." -Level WARN }
        } else { Write-Log "Invalid input, skipping." -Level WARN }
    }
    return $modelMap
}
function Run-ConfigMode {
    param([string]$MappingFilePath)
    Write-Log -Message "Running in CONFIG mode."
    $devices = Get-NinjaDevicesDetailed; if (-not $devices) { throw "No NinjaOne devices found." }
    $assetTypes = (Invoke-FreshApi -Method GET -Path "/asset_types").asset_types; if (-not $assetTypes) { throw "No Freshservice asset types found." }
    $products = Get-FreshProducts
    $ninjaFieldCandidates = ($devices | Select-Object -First 1).PSObject.Properties.Name | Sort-Object -Unique
    $roleMap = Configure-RoleMappings -Devices $devices -AssetTypes $assetTypes -NinjaFieldCandidates $ninjaFieldCandidates
    $modelMap = if ($products) { Configure-ModelToProduct -Devices $devices -Products $products } else { @{} }
    $configOut = [ordered]@{ mappings=[ordered]@{ roleToAssetType=$roleMap; modelToProduct=$modelMap }; generatedAtUtc=(Get-Date).ToUniversalTime().ToString("o") }
    Set-Content -Path $MappingFilePath -Value ($configOut | ConvertTo-Json -Depth 10) -Encoding UTF8
    Write-Log -Message "Configuration complete. Saved mapping file to: $MappingFilePath"
}

# ==========================================================
# SYNC MODE
# ==========================================================
function Build-ExistingAssetIndex {
    param([object[]]$Assets, [hashtable]$RoleToAssetType)
    Write-Log -Message "Indexing existing assets by (asset_type_id, unique_id)..."
    $uniqueIdFieldNames = $RoleToAssetType.GetEnumerator().Value.unique_identifier_mapping.fs_name | Select-Object -Unique
    $index = @{}
    foreach ($asset in $Assets) {
        if (-not $asset.asset_type_id) { continue }
        foreach ($fieldName in $uniqueIdFieldNames) {
            $value = $null
            if ($fieldName -eq 'serial_number') {
                $value = $asset.serial_number
            } elseif ($asset.custom_fields -and $asset.custom_fields.PSObject.Properties[$fieldName]) {
                $value = $asset.custom_fields.$fieldName
            }
            if ($value) {
                $normalizedValue = Normalize-Identifier -Identifier $value
                if ($normalizedValue) {
                    $key = "$($asset.asset_type_id)|$normalizedValue"
                    if (-not $index.ContainsKey($key)) { $index[$key] = $asset }
                }
            }
        }
    }
    Write-Log -Message "Indexed $($index.Keys.Count) unique asset entries."
    return $index
}
function Run-SyncMode {
    param([string]$MappingFilePath, [switch]$DryRun, [switch]$UpdateExisting)
    $stateFilePath = Join-Path -Path $PSScriptRoot -ChildPath "state.json"
    $lastSyncTimestampUnix = 0
    if (Test-Path $stateFilePath) {
        try {
            $lastSyncUtc = [datetimeoffset]::Parse((Get-Content -Path $stateFilePath | ConvertFrom-Json).lastSuccessfulSyncUtc)
            $lastSyncTimestampUnix = $lastSyncUtc.ToUnixTimeSeconds()
            Write-Log -Message "Found previous sync state. Processing devices updated since $($lastSyncUtc.ToString('o'))"
        } catch { Write-Log "Could not parse state.json. Performing a full sync." -Level WARN }
    } else { Write-Log "No state file found. Performing a full sync." }
    
    $syncStartTime = (Get-Date).ToUniversalTime()
    if (-not (Test-Path $MappingFilePath)) { throw "Mapping file not found: $MappingFilePath" }
    $mapping = Get-Content -Path $MappingFilePath -Raw | ConvertFrom-Json
    $roleToAssetType = $mapping.mappings.roleToAssetType
    $modelToProduct = $mapping.mappings.modelToProduct
    
    $allDevices = Get-NinjaDevicesDetailed
    $devicesToProcess = $allDevices | Where-Object { $_.lastUpdate -gt $lastSyncTimestampUnix }
    Write-Log -Message "$($devicesToProcess.Count) of $($allDevices.Count) total devices to process."

    if ($devicesToProcess.Count -eq 0) {
        Write-Log -Message "No new device updates to process. Sync is complete."
        return
    }

    $existingAssets = Get-FreshAssetsExisting
    $allProducts = Get-FreshProducts
    $existingAssetIndex = Build-ExistingAssetIndex -Assets $existingAssets -RoleToAssetType $roleToAssetType
    $stats = @{ created=0; updated=0; skipped_mapping=0; skipped_id=0; skipped_exist=0; errors=0 }
    
    $i = 0
    foreach ($device in $devicesToProcess) {
        $i++; Write-Progress -Activity "Syncing devices" -Status "Processing $i of $($devicesToProcess.Count)" -PercentComplete (($i / $devicesToProcess.Count) * 100)
        try {
            $role = $device.nodeClass
            if (-not $role -or -not $roleToAssetType.$role) { $stats.skipped_mapping++; continue }
            $roleConfig = $roleToAssetType.$role
            if (-not $roleConfig.unique_identifier_mapping) { $stats.skipped_mapping++; continue }
            
            $rawUniqueIdValue = Get-NinjaFieldValue -Device $device -Path $roleConfig.unique_identifier_mapping.ninja_field
            $uniqueIdValue = Normalize-Identifier -Identifier $rawUniqueIdValue
            if (-not $uniqueIdValue) { $stats.skipped_id++; continue }

            $key = "$($roleConfig.asset_type_id)|$uniqueIdValue"
            $existingAsset = $existingAssetIndex[$key]
            $body = @{ asset_type_id = $roleConfig.asset_type_id }
            $model = $device.system.model
            if ($model) {
                if ($modelToProduct.ContainsKey($model)) {
                    $body.product_id = $modelToProduct[$model].product_id
                } else {
                    Write-Log -Message "Model '$model' not in config. Searching/creating in Freshservice..."
                    $existingProd = $allProducts | Where-Object { $_.name -eq $model }
                    if ($existingProd) {
                        $body.product_id = $existingProd.id
                        $modelToProduct[$model] = @{ product_id=$existingProd.id; product_name=$existingProd.name }
                    } else {
                        try {
                            $newProdBody = @{ product = @{ name=$model; manufacturer=$device.system.manufacturer } }
                            $newProdResult = Invoke-FreshApi -Method POST -Path "/products" -Body $newProdBody
                            $createdProd = $newProdResult.product
                            $body.product_id = $createdProd.id
                            $allProducts += $createdProd
                            $modelToProduct[$model] = @{ product_id=$createdProd.id; product_name=$createdProd.name }
                            Write-Log -Message "Auto-created product '$($createdProd.name)' with ID $($createdProd.id)." -Level WARN
                        } catch { Write-Log -Message "Failed to auto-create product for '$model': $($_.Exception.Message)" -Level ERROR }
                    }
                }
            }
            foreach ($map in $roleConfig.field_mappings) {
                $rawValue = if ($map.fs_name -eq "name") { $device.systemName -or "Ninja device $($device.id)" } `
                            else { Get-NinjaFieldValue -Device $device -Path $map.ninja_field }
                if ($rawValue -and $rawValue -ne "") {
                    if ($body.PSObject.Properties.Name.Contains($map.fs_name)) { $body[$map.fs_name] = $rawValue }
                    else { if (-not $body.custom_fields) { $body.custom_fields = @{} }; $body.custom_fields[$map.fs_name] = $rawValue }
                }
            }
            if ($existingAsset) {
                if (-not $UpdateExisting) { $stats.skipped_exist++; continue }
                if ($DryRun) { $stats.updated++; Write-Log -Message "DRY RUN - Would UPDATE asset id $($existingAsset.id)" -Level WARN; continue }
                Invoke-FreshApi -Method PUT -Path "/assets/$($existingAsset.id)" -Body $body
                $stats.updated++; Write-Log -Message "Updated asset id $($existingAsset.id) for Ninja device $($device.id)."
            } else {
                if ($DryRun) { $stats.created++; Write-Log -Message "DRY RUN - Would CREATE asset for Ninja device $($device.id)" -Level WARN; continue }
                $resp = Invoke-FreshApi -Method POST -Path "/assets" -Body $body
                $stats.created++; Write-Log -Message "Created asset for $($device.id) -> FS asset id $($resp.asset.id)"
                if ($resp.asset -and -not $existingAssetIndex.ContainsKey($key)) { $existingAssetIndex[$key] = $resp.asset }
            }
        } catch {
            $stats.errors++
            Write-Log -Message "Unhandled error for Ninja device ID $($device.id): $($_.ToString())" -Level ERROR
        }
    }
    
    $stats.skipped_total = $stats.skipped_mapping + $stats.skipped_id + $stats.skipped_exist
    Write-Log -Message "Sync complete."
    if ($stats.errors -eq 0) {
        Write-Log -Message "Sync successful. Updating state file."
        @{ lastSuccessfulSyncUtc = $syncStartTime.ToString("o") } | ConvertTo-Json | Set-Content -Path $stateFilePath
    } else {
        Write-Log -Message "Sync completed with errors. State file will NOT be updated." -Level WARN
    }
    
    $summaryTitle = if ($DryRun) { "Freshservice Sync Dry Run Complete" } else { "Freshservice Sync Complete" }
    if($stats.errors -gt 0) { $summaryTitle += " with ERRORS" }
    $summaryMessage = "Processed: $($devicesToProcess.Count)`nCreated: $($stats.created)`nUpdated: $($stats.updated)`nSkipped: $($stats.skipped_total)`nErrors: $($stats.errors)"
    $summaryColor = if ($stats.errors -gt 0) { "danger" } else { "good" }
    Send-WebhookNotification -Title $summaryTitle -Message $summaryMessage -Color $summaryColor
}

# ==========================================================
# MAIN ENTRYPOINT
# ==========================================================
try {
    Write-Log -Message "Script execution started. Mode: $Mode"
    if (-not (Test-Path -Path $ConfigFilePath)) { throw "Configuration file not found at $ConfigFilePath" }
    $script:Config = Get-Content -Path $ConfigFilePath -Raw | ConvertFrom-Json

    $script:SecretManagementAvailable = (Get-Module -ListAvailable -Name Microsoft.PowerShell.SecretManagement) -ne $null
    if ($script:SecretManagementAvailable) { Write-Log "PowerShell SecretManagement module detected." -Level DEBUG }

    $ninjaClientId = Resolve-Credential -ParamValue $NinjaClientId -SecretName $script:Config.Secrets.NinjaClientId -EnvVarName "NINJAONE_CLIENT_ID"
    $ninjaClientSecret = Resolve-Credential -ParamValue $NinjaClientSecret -SecretName $script:Config.Secrets.NinjaClientSecret -EnvVarName "NINJAONE_CLIENT_SECRET"
    $script:FreshApiKey = Resolve-Credential -ParamValue $FreshApiKey -SecretName $script:Config.Secrets.FreshApiKey -EnvVarName "FRESHSERVICE_API_KEY"
    
    if (-not ($ninjaClientId -and $ninjaClientSecret -and $script:FreshApiKey)) {
        throw "Could not resolve all required credentials."
    }

    $script:NinjaBaseUri = Get-NinjaBaseUri -Region $script:Config.NinjaRegion
    $script:NinjaAccessToken = Get-NinjaAccessToken -BaseUri $script:NinjaBaseUri -ClientId $ninjaClientId -ClientSecret $ninjaClientSecret
    
    $mappingFilePath = Join-Path -Path $PSScriptRoot -ChildPath "ninja_fs_mapping.json"

    if ($Mode -eq "config") {
        Run-ConfigMode -MappingFilePath $mappingFilePath
    } elseif ($Mode -eq "sync") {
        Run-SyncMode -MappingFilePath $mappingFilePath -DryRun:$DryRun -UpdateExisting:$UpdateExisting
    }
    
    Write-Log -Message "Script execution finished successfully."
} catch {
    $errorMessage = $_.ToString()
    Write-Log -Message "A critical error occurred: $errorMessage" -Level ERROR
    Send-WebhookNotification -Title "Freshservice Sync FAILED" -Message $errorMessage -Color "danger"
    exit 1
}