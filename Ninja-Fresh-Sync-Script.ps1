#requires -Version 5.1
<#
.SYNOPSIS
An enterprise-grade script to configure and synchronize NinjaOne devices as assets in Freshservice.

.DESCRIPTION
This script is designed for secure, automated, and scheduled execution. It uses an external config file,
supports the PowerShell SecretManagement module, performs efficient delta-syncing, and provides webhook notifications.
This version is fully compatible with Windows PowerShell 5.1.

.PARAMETER Mode
Specifies the operational mode. 'config' or 'sync'.

.PARAMETER ConfigFilePath
Path to the `config.json` file. Defaults to '.\config.json'.

.PARAMETER NinjaRegion
(Optional) Overrides the NinjaRegion value from config.json.

.PARAMETER FreshSubdomain
(Optional) Overrides the FreshSubdomain value from config.json.

.PARAMETER NinjaClientId
(Optional) Override the Client ID from the config or secret vault.

.PARAMETER NinjaClientSecret
(Optional) Override the Client Secret from the config or secret vault.

.PARAMETER FreshApiKey
(Optional) Override the Freshservice API Key from the config or secret vault.

.PARAMETER DryRun
(Switch) In 'sync' mode, logs actions without making changes to Freshservice.

.PARAMETER UpdateExisting
(Switch) In 'sync' mode, allows the script to update existing assets.

.PARAMETER DebugMode
(Switch) Enables verbose debug logging, including API payloads.

.EXAMPLE
# Run the configuration wizard (provide credentials manually for this one-time run)
.\Sync-Script.ps1 -Mode config -NinjaClientId "..." -NinjaClientSecret "..." -FreshApiKey "..."

.EXAMPLE
# Run the automated sync using credentials from a secure vault with a dry run.
.\Sync-Script.ps1 -Mode sync -DryRun

.NOTES
Disclaimer: Human-authored, with assistance from code completion tools.
Version: 8.2
#>

[CmdletBinding()]
param(
    [ValidateSet("config","sync")]
    [string]$Mode            = "config",
    [switch]$DebugMode,
    [string]$ConfigFilePath  = (Join-Path -Path $PSScriptRoot -ChildPath "config.json"),

    # Optional overrides for configuration and credentials
    [string]$NinjaRegion,
    [string]$FreshSubdomain,
    [string]$NinjaClientId,
    [string]$NinjaClientSecret,
    [string]$FreshApiKey,

    # Switches for sync mode
    [switch]$DryRun,
    [switch]$UpdateExisting
)

# ==========================================================
# LOGGING & WEBHOOK SETUP
# ==========================================================
$LogDirectory = Join-Path -Path $PSScriptRoot -ChildPath "logs"
if (-not (Test-Path -Path $LogDirectory)) { New-Item -Path $LogDirectory -ItemType Directory | Out-Null }
$script:LogFile = Join-Path -Path $LogDirectory -ChildPath "sync-log-$(Get-Date -Format 'yyyy-MM-dd').log"

function Write-Log {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Message,
        [ValidateSet('INFO', 'WARN', 'ERROR', 'DEBUG')]
        [string]$Level = 'INFO'
    )

    if ($Level -eq 'DEBUG' -and -not $script:DebugMode) {
        return
    }

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
    if ([string]::IsNullOrWhiteSpace($script:Config.Webhook.Uri)) { return }
    
    $payload = $null
    $webhookFormat = $script:Config.Webhook.Format

    if ($webhookFormat -eq 'Teams') {
        $cardColor = switch ($Color) { 'danger' { 'Attention' } 'good'   { 'Good' } default  { 'Default' } }
        $formattedMessage = $Message -replace "`n", "`r`n"
        $payload = @{
            type = 'message'
            attachments = @( @{
                    contentType = 'application/vnd.microsoft.card.adaptive'
                    content = @{
                        type    = 'AdaptiveCard'; version = '1.4'; schema  = 'http://adaptivecards.io/schemas/adaptive-card.json'
                        body    = @(
                            @{ type = 'TextBlock'; text = $Title; weight = 'Bolder'; size = 'Large'; color  = $cardColor },
                            @{ type = 'TextBlock'; text = $formattedMessage; wrap = $true }
                        )
                    }
            })
        } | ConvertTo-Json -Depth 5
    } elseif ($webhookFormat -eq 'Slack') {
        $payload = @{
            title        = $Title
            message      = $Message
            status_color = $Color
        } | ConvertTo-Json
        # $payload = @{ attachments = @( @{ title = $Title; text = $Message; color = $Color } ) } | ConvertTo-Json #if using slackbot
    } else {
        Write-Log -Message "Webhook format '$($webhookFormat)' is unknown. Notification not sent." -Level WARN
        return
    }

    try {
        Invoke-RestMethod -Uri $script:Config.Webhook.Uri -Method Post -Body $payload -ContentType 'application/json' -ErrorAction Stop
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
            
            if ($_.Exception.Response) {
                try {
                    $stream = $_.Exception.Response.GetResponseStream()
                    $reader = New-Object System.IO.StreamReader($stream)
                    $errorResponse = $reader.ReadToEnd()
                    $reader.Close()
                    $stream.Close()
                } catch {
                    $errorResponse = "Could not read error response stream."
                }
                Write-Log -Message "API Error Response Body: $errorResponse" -Level ERROR
            }

            if ($statusCode -in 400,401,403,404) { Write-Log -Message "Non-retriable status code $statusCode." -Level ERROR; throw }
            if ($attempt -eq $MaxRetries) { Write-Log -Message "Final API attempt failed." -Level ERROR; throw }
            $backoffSeconds = 0
            if ($statusCode -eq 429 -and $_.Exception.Response.Headers['Retry-After']) {
                $backoffSeconds = [int]$_.Exception.Response.Headers['Retry-After']
            } else {
                $backoffSeconds = [int]([Math]::Min($InitialRetryDelay * ([Math]::Pow(2, $attempt - 1)), 300))
                $backoffSeconds += Get-Random -Minimum 0 -Maximum ([Math]::Ceiling($backoffSeconds * 0.1))
            }
            Write-Log -Message "API call failed (Status: $statusCode). Retrying in $backoffSeconds seconds..." -Level WARN
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
    param([string]$ParamValue, [string]$ConfigKeyName, [string]$SecretName, [string]$EnvVarName)
    
    # 1. Check for command-line parameter (highest priority)
    if (-not [string]::IsNullOrWhiteSpace($ParamValue)) { return $ParamValue }

    $secretConfigProperties = $script:Config.Secrets.PSObject.Properties.Name

    # 2. Check config.json directly if UseSecretManagement is false
    if ($secretConfigProperties -contains 'UseSecretManagement' -and -not $script:Config.Secrets.UseSecretManagement) {
        if ($secretConfigProperties -contains $ConfigKeyName) {
            $configValue = $script:Config.Secrets.$ConfigKeyName
            if (-not [string]::IsNullOrWhiteSpace($configValue)) {
                return $configValue
            }
        }
    }

    # 3. Check PowerShell SecretManagement vault (if enabled)
    if ($secretConfigProperties -contains 'UseSecretManagement' -and $script:Config.Secrets.UseSecretManagement -and $script:SecretManagementAvailable) {
        try {
            return Get-Secret -Name $SecretName -AsPlainText -ErrorAction Stop
        }
        catch {
            Write-Log -Message "Secret '$SecretName' not found in vault. Checking environment variables." -Level DEBUG
        }
    }

    # 4. Check Environment Variable (lowest priority)
    return (Get-Item "env:\$EnvVarName" -ErrorAction SilentlyContinue).Value
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
    $uriBuilder = New-Object System.UriBuilder -ArgumentList "$BaseUri$Path"
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
    $headers = @{ "Authorization" = "Basic " + [Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes("$($script:FreshApiKey):X")); "Accept" = "application/json" }
    $resp = Invoke-ApiCall -BaseUri "https://$($script:Config.FreshSubdomain).freshservice.com/api/v2" -Method $Method -Path $Path -Headers $headers -Query $Query -Body $Body
    if (-not [string]::IsNullOrWhiteSpace($resp.Content)) {
        return $resp.Content | ConvertFrom-Json
    }
    return $null
}
function Get-FreshAssetTypes {
    Write-Log -Message "Loading Freshservice asset types..."
    $allAssetTypes = @()
    $page = 1
    $perPage = 100
    do {
        $result = Invoke-FreshApi -Method GET -Path "/asset_types" -Query @{ per_page = $perPage; page = $page }
        if ($result.asset_types) {
            $allAssetTypes += $result.asset_types
            $count = ($result.asset_types | Measure-Object).Count
            Write-Log -Message "Fetched page $page with $count asset types." -Level DEBUG
        } else {
            $count = 0
        }
        $page++
    } while ($count -eq $perPage)
    
    Write-Log -Message "Total Freshservice asset types fetched: $($allAssetTypes.Count)"
    return $allAssetTypes
}
function Get-FreshAssetsExisting {
    Write-Log -Message "Fetching existing Freshservice assets..."
    $allAssets = @()
    $nextUrl = "https://$($script:Config.FreshSubdomain).freshservice.com/api/v2/assets?include=type_fields&per_page=100"
    $headers = @{ "Authorization" = "Basic " + [Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes("$($script:FreshApiKey):X")); "Accept" = "application/json" }
    while ($nextUrl) {
        $params = @{ Uri = $nextUrl; Headers = $headers; Method = "GET" }
        $resp = Invoke-RobustApiCall -Params $params
        $json = $resp.Content | ConvertFrom-Json
        if ($json.assets) { $allAssets += $json.assets }
        $nextUrl = if ($resp.Headers["Link"] -match '<(.*?)>;\s*rel="next"') { $matches[1] } else { $null }
    }
    Write-Log -Message "Total Freshservice assets fetched: $($allAssets.Count)"
    return $allAssets
}
function Get-NinjaDevicesDetailed {
    Write-Log -Message "Fetching NinjaOne devices..."
    $allDevices = New-Object System.Collections.Generic.List[object]
    $after = $null
    do {
        $resp = Invoke-NinjaApi -Method GET -Path "/api/v2/devices-detailed" -Query @{ pageSize = 1000; after = $after }
        
        $batch = $null
        if ($resp -is [array]) {
            $batch = $resp
        }
        elseif ($resp.PSObject.Properties.Match("items")) {
            $batch = @($resp.items)
        }
        else {
            $batch = @($resp)
        }

        if ($batch) {
            $allDevices.AddRange($batch)
        }

        $after = if ($batch.Count -eq 1000) { $batch[-1].id } else { $null }
    } while ($after)
    Write-Log -Message "Total NinjaOne devices fetched: $($allDevices.Count)"
    return $allDevices
}
function Get-FreshProducts {
    Write-Log -Message "Loading Freshservice products..."
    $allProducts = @()
    $page = 1
    $perPage = 100
    do {
        $result = Invoke-FreshApi -Method GET -Path "/products" -Query @{ per_page = $perPage; page = $page }
        if ($result.products) {
            $allProducts += $result.products
            $count = ($result.products | Measure-Object).Count
            Write-Log -Message "Fetched page $page with $count products." -Level DEBUG
        } else {
            $count = 0
        }
        $page++
    } while ($count -eq $perPage)
    Write-Log -Message "Total Freshservice products fetched: $($allProducts.Count)"
    return $allProducts
}
function Get-FreshAssetTypeFields {
    param([long]$AssetTypeId)
    Write-Log -Message "Loading Freshservice asset type fields for type ID $AssetTypeId..." -Level DEBUG
    try {
        $result = Invoke-FreshApi -Method GET -Path "/asset_types/$($AssetTypeId)/fields" -ErrorAction Stop
        if ($result.asset_type_fields.fields) {
            return $result.asset_type_fields.fields
        }
    } catch {
        Write-Log -Message "Failed to fetch fields for asset type ${AssetTypeId}: $($_.Exception.Message)" -Level WARN
    }
    return @()
}
function Get-ObjectPaths-Recursive {
    param($InputObject, $Prefix, $PathList)
    if ($null -eq $InputObject) { return }

    if ($InputObject -is [array]) {
        if ($InputObject.Count -gt 0) {
            Get-ObjectPaths-Recursive -InputObject $InputObject[0] -Prefix "$Prefix[]" -PathList $PathList
        }
    } elseif ($InputObject.PSObject.BaseObject -is [System.Management.Automation.PSCustomObject]) {
        foreach ($prop in $InputObject.PSObject.Properties) {
            $currentPath = if ([string]::IsNullOrEmpty($Prefix)) { $prop.Name } else { "$Prefix.$($prop.Name)" }
            $PathList.Add($currentPath) | Out-Null
            Get-ObjectPaths-Recursive -InputObject $prop.Value -Prefix $currentPath -PathList $PathList
        }
    }
}
function Get-NinjaFieldCandidates {
    param([object[]]$Devices)
    $sample = $Devices | Select-Object -First 1
    if (-not $sample) { return @() }
    $pathList = New-Object System.Collections.Generic.List[string]
    Get-ObjectPaths-Recursive -InputObject $sample -Prefix "" -PathList $pathList
    return $pathList | Sort-Object -Unique
}
function Get-NinjaFieldValue {
    param([psobject]$Device, [string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path)) { return $null }

    $parts = $Path -split '\.'
    $currentObject = $Device
    
    for ($i = 0; $i -lt $parts.Length; $i++) {
        $part = $parts[$i]
        if ($null -eq $currentObject) { return $null }

        if ($part.EndsWith("[]")) {
            $propName = $part.Substring(0, $part.Length - 2)
            if ($currentObject.PSObject.Properties[$propName] -and $currentObject.$propName -is [array]) {
                $array = $currentObject.$propName
                $remainingPath = $parts[($i+1)..$parts.Length] -join '.'
                $values = @()
                foreach ($item in $array) {
                    $values += Get-NinjaFieldValue -Device $item -Path $remainingPath
                }
                return $values -join ', '
            } else {
                return $null
            }
        }
        
        if ($currentObject.PSObject.Properties.Name -notcontains $part) {
            return $null
        }
        $currentObject = $currentObject.$part
    }
    return $currentObject
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
        if (($input -match '^\d+$') -and ([int]$input -ge 0) -and ([int]$input -lt $Items.Count)) { return $Items[[int]$input] }
        Write-Log -Message "Invalid selection, try again." -Level WARN
    }
}
function Prompt-NinjaFieldForFsField {
    param([psobject]$FsField, [string[]]$Candidates)
    Write-Log -Message "`nFreshservice required field: $($FsField.label) (name: $($FsField.name))"
    for ($i=0;$i -lt $Candidates.Count;$i++) { Write-Log -Message "[$i] $($Candidates[$i])" }
    $input = Read-Host "Select NinjaOne field index, enter a custom path, or press Enter to skip"
    if ([string]::IsNullOrWhiteSpace($input)) { return $null }
    if (($input -match '^\d+$') -and ([int]$input -ge 0) -and ([int]$input -lt $Candidates.Count)) { return $Candidates[[int]$input] }
    return $input
}
function Run-InteractiveConfiguration {
    param(
        [Parameter(Mandatory=$true)][object[]]$Devices,
        [Parameter(Mandatory=$true)][object[]]$AssetTypes,
        [Parameter(Mandatory=$true)][object[]]$Products,
        [Parameter(Mandatory=$true)][string[]]$NinjaFieldCandidates
    )
    $roleMap = @{}
    $modelMap = @{}
    
    $roles = $Devices.nodeClass | Where-Object {$_} | Sort-Object -Unique
    if (-not $roles) { Write-Log "No nodeClass values found on NinjaOne devices." -Level WARN; return @{ RoleMap = @{}; ModelMap = @{} } }

    foreach ($role in $roles) {
        Write-Log -Message "`n=================================================="
        Write-Log -Message "Configuring Role: $role"
        Write-Log -Message "=================================================="
        
        $assetType = Choose-FromList -Items $AssetTypes -DisplayProperty "name" -Prompt "Step 1: Select a Freshservice Asset Type for the '$role' role" -AllowSkip
        if (-not $assetType) { Write-Log "Skipping role '$role'." -Level WARN; continue }

        $sampleDevice = $Devices | Where-Object { $_.nodeClass -eq $role } | Select-Object -First 1
        if ($sampleDevice) {
            Write-Log -Message "`nHere is an example of the data available for devices with the '$role' role. Use this as a reference for mapping fields." -Level INFO
            $sampleDevice | ConvertTo-Json -Depth 10 | Write-Host
        }

        Write-Log -Message "`nStep 2: Map Freshservice fields for Asset Type '$($assetType.name)'"
        $fieldGroups = Invoke-FreshApi -Method GET -Path "/asset_types/$($assetType.id)/fields"
        $allFields = $fieldGroups.asset_type_fields.fields | ForEach-Object {$_}
        $fieldMappings = @()
        
        Write-Log -Message "`n--- Mapping REQUIRED Fields ---" -Level INFO
        $requiredFields = $allFields | Where-Object {$_.required}
        foreach ($rf in $requiredFields) {
            if ($rf.name -eq "asset_type_id" -or $rf.name -match '^product_\d+$') { continue }
            if ($rf.choices -and ($rf.choices | Measure-Object).Count -gt 0) {
                Write-Log -Message "`nField '$($rf.label)' has predefined choices. Select a default value for NEW assets."
                $choiceObjects = $rf.choices | ForEach-Object { [pscustomobject]@{ Display = $_[0]; Value = $_[1] } }
                $chosen = Choose-FromList -Items $choiceObjects -DisplayProperty 'Display' -Prompt "Select a default for '$($rf.label)'"
                if ($chosen) { $fieldMappings += @{ fs_name=$rf.name; fs_label=$rf.label; static_value=$chosen.Display } }
            } else {
                $ninjaPath = if ($rf.name -eq "name") { "systemName" } else { Prompt-NinjaFieldForFsField -FsField $rf -Candidates $NinjaFieldCandidates }
                if ($ninjaPath) { $fieldMappings += @{ fs_name=$rf.name; fs_label=$rf.label; ninja_field=$ninjaPath } }
            }
        }

        Write-Log -Message "`n--- Mapping OPTIONAL Fields (for Unique ID selection and data sync) ---" -Level INFO
        $optionalFields = $allFields | Where-Object { -not $_.required -and -not ($_.name -match '^product_\d+$') -and -not ($_.name -eq 'asset_type_id') }
        $mappedOptionalFieldNames = @()
        while ((Read-Host -Prompt "Map an optional field? (y/n)").ToLower() -eq 'y') {
            $unmappedOptionalFields = $optionalFields | Where-Object { $_.name -notin $mappedOptionalFieldNames }
            if (-not $unmappedOptionalFields) { Write-Log -Message "No more optional fields to map." -Level WARN; break }
            $fieldToMap = Choose-FromList -Items $unmappedOptionalFields -DisplayProperty "label" -Prompt "Select optional field to map"
            if ($fieldToMap) {
                $mappedOptionalFieldNames += $fieldToMap.name
                $ninjaPath = Prompt-NinjaFieldForFsField -FsField $fieldToMap -Candidates $NinjaFieldCandidates
                if ($ninjaPath) { $fieldMappings += @{ fs_name=$fieldToMap.name; fs_label=$fieldToMap.label; ninja_field=$ninjaPath } }
            }
        }

        $uniqueIdMapping = $null
        $mappableFields = $fieldMappings | Where-Object { $_.ninja_field }
        if ($mappableFields) {
            Write-Log -Message "`nStep 3: Choose the Unique Identifier for role '$role'"
            $choices = $mappableFields | ForEach-Object {[pscustomobject]@{Display="FS:'$($_.fs_label)'<-Ninja:'$($_.ninja_field)'";Mapping=$_}}
            $chosen = Choose-FromList -Items $choices -DisplayProperty "Display" -Prompt "Select unique identifier"
            $uniqueIdMapping = if ($chosen) { $chosen.Mapping }
            if ($uniqueIdMapping) { Write-Log "Unique ID for '$role' set to FS field '$($uniqueIdMapping.fs_name)'." }
        } else { Write-Log -Message "No fields were mapped from NinjaOne, cannot select a unique identifier." -Level WARN }
        
        $productField = $allFields | Where-Object { $_.name -match '^product_\d+$' } | Select-Object -First 1
        $productFieldName = if ($productField) { $productField.name } else { $null }
        if ($productFieldName) { Write-Log "Discovered product field name: '$productFieldName'" -Level DEBUG }

        $roleMap[$role] = @{ 
            asset_type_id = $assetType.id; asset_type_name = $assetType.name
            unique_identifier_mapping = $uniqueIdMapping; field_mappings = $fieldMappings
            product_field_name = $productFieldName
        }

        $modelsForThisRole = $Devices | Where-Object { $_.nodeClass -eq $role -and $_.system.model } | Select-Object @{N='Mfr';E={$_.system.manufacturer}},@{N='Model';E={$_.system.model}} | Group-Object Model,Mfr | ForEach-Object {$_.Group[0]}
        if ($modelsForThisRole) {
            Write-Log -Message "`nStep 4: Map device models for role '$role' to Freshservice Products"
            $relevantProducts = $Products | Where-Object { $_.asset_type_id -eq $assetType.id }
            $previewProducts = $relevantProducts | Select-Object -First 50
            if ($previewProducts) {
                Write-Log -Message "Showing products of type '$($assetType.name)' (first 50):"
                for ($i=0; $i -lt $previewProducts.Count; $i++) { Write-Log -Message "[$i] $($previewProducts[$i].name) (id $($previewProducts[$i].id))" }
            }
            foreach ($m in $modelsForThisRole) {
                if ($modelMap.ContainsKey($m.Model)) { continue }
                Write-Log -Message "`nModel: $($m.Model) (Manufacturer: $($m.Mfr))"
                $input = Read-Host "Enter list index, full product id, (S)kip, (C)reate, or Enter to choose from list"
                if ($input -in 'S','s') { Write-Log "Skipping '$($m.Model)'." -Level WARN; continue }
                if ($input -in 'C','c') {
                    $existing = $Products | Where-Object {$_.name -eq $m.Model} | Select-Object -First 1
                    if ($existing) { $modelMap[$m.Model] = @{product_id=$existing.id;product_name=$existing.name} }
                    else {
                        try {
                            $newProdBody = @{name=$m.Model;manufacturer=$m.Mfr;asset_type_id=$assetType.id}
                            $newProd = Invoke-FreshApi -Method POST -Path "/products" -Body $newProdBody
                            $modelMap[$m.Model] = @{product_id=$newProd.product.id;product_name=$newProd.product.name}
                            $Products += $newProd.product
                        } catch { Write-Log "Failed to create product '$($m.Model)': $($_.Exception.Message)" -Level ERROR }
                    }
                } elseif ([string]::IsNullOrWhiteSpace($input)) {
                    $idx = Read-Host "Select preview index for product (0..$($previewProducts.Count-1), or S to skip)"
                    if ($idx -in 'S','s') { continue }
                    if (($idx -match '^\d+$') -and ([int]$idx -ge 0) -and ([int]$idx -lt $previewProducts.Count)) {
                        $prod = $previewProducts[[int]$idx]; $modelMap[$m.Model] = @{ product_id = $prod.id; product_name = $prod.name }
                    } else { Write-Log "Invalid index, skipping." -Level WARN }
                } elseif ($input -match '^\d+$') {
                    $numericInput = [int]$input; $prod = $null
                    if ($previewProducts -and $numericInput -ge 0 -and $numericInput -lt $previewProducts.Count) { $prod = $previewProducts[$numericInput] } 
                    else { $prod = $Products | Where-Object { $_.id -eq [long]$input } | Select-Object -First 1 }
                    if ($prod) { $modelMap[$m.Model] = @{ product_id = $prod.id; product_name = $prod.name }; Write-Log -Message "Mapped '$($m.Model)' to '$($prod.name)'." -Level INFO }
                    else { Write-Log "No product found for index or ID '$input'. Skipping." -Level WARN }
                } else { Write-Log "Invalid input." -Level WARN }
            }
        }
    }
    return @{ RoleMap = $roleMap; ModelMap = $modelMap }
}
function Run-ConfigMode {
    param([string]$MappingFilePath)
    Write-Log -Message "Running in CONFIG mode."
    $devices = Get-NinjaDevicesDetailed; if (-not $devices) { throw "No NinjaOne devices found." }
    $assetTypes = Get-FreshAssetTypes; if (-not $assetTypes) { throw "No Freshservice asset types found." }
    $products = Get-FreshProducts
    $ninjaFieldCandidates = Get-NinjaFieldCandidates -Devices $devices
    
    $maps = Run-InteractiveConfiguration -Devices $devices -AssetTypes $assetTypes -Products $products -NinjaFieldCandidates $ninjaFieldCandidates
    
    $configOut = [ordered]@{ mappings=[ordered]@{ roleToAssetType=$maps.RoleMap; modelToProduct=$maps.ModelMap }; generatedAtUtc=(Get-Date).ToUniversalTime().ToString("o") }
    Set-Content -Path $MappingFilePath -Value ($configOut | ConvertTo-Json -Depth 10) -Encoding UTF8
    Write-Log -Message "Configuration complete. Saved mapping file to: $MappingFilePath"
}

# ==========================================================
# SYNC MODE
# ==========================================================
function Build-ExistingAssetIndex {
    param(
        [Parameter(Mandatory=$true)][object[]]$Assets,
        [Parameter(Mandatory=$true)][hashtable]$RoleToAssetType
    )
    Write-Log -Message "Indexing existing assets using explicit unique field from mapping file..."

    # 1. Create a lookup map using the exact 'fs_name' from the configuration.
    $uniqueFieldMap = @{}
    foreach ($role in $RoleToAssetType.GetEnumerator()) {
        $assetTypeId = $role.Value.asset_type_id
        if ($role.Value.unique_identifier_mapping -and $role.Value.unique_identifier_mapping.fs_name) {
            $uniqueFieldName = $role.Value.unique_identifier_mapping.fs_name
            
            if (-not $uniqueFieldMap.ContainsKey($assetTypeId)) {
                $uniqueFieldMap[$assetTypeId] = $uniqueFieldName
                Write-Log -Message "Asset type ID $assetTypeId will be indexed by field: '$uniqueFieldName'" -Level DEBUG
            }
        } else {
            Write-Log -Message "No unique identifier mapping configured for role $($role.Name). Assets of this type cannot be indexed for updates." -Level WARN
        }
    }

    # 2. Build the index using the exact field name.
    $index = @{}
    foreach ($asset in $Assets) {
        $assetTypeId = $asset.asset_type_id
        if (-not $assetTypeId -or -not $uniqueFieldMap.ContainsKey($assetTypeId)) {
            continue
        }

        $fieldName = $uniqueFieldMap[$assetTypeId]
        $value = $null

        # Check if the field is a top-level property (like 'name' or 'asset_tag')
        if ($asset.PSObject.Properties.Name -contains $fieldName) {
            $value = $asset.$fieldName
        }
        # Check if the field is a type specific custom field inside 'type_fields'
        elseif ($asset.type_fields -and $asset.type_fields.PSObject.Properties.Name -contains $fieldName) {
            $value = $asset.type_fields.$fieldName
        }

        if ($value) {
            $normalizedValue = Normalize-Identifier -Identifier ([string]$value)
            if ($normalizedValue) {
                $key = "$($assetTypeId)|$normalizedValue"
                if (-not $index.ContainsKey($key)) { $index[$key] = $asset }
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
    
    $roleToAssetType = @{}; if ($mapping.mappings.roleToAssetType) { $mapping.mappings.roleToAssetType.psobject.Properties | ForEach-Object { $roleToAssetType[$_.Name] = $_.Value } }
    $modelToProduct = @{}; if ($mapping.mappings.modelToProduct) { $mapping.mappings.modelToProduct.psobject.Properties | ForEach-Object { $modelToProduct[$_.Name] = $_.Value } }
    
    $allDevices = Get-NinjaDevicesDetailed
    $devicesToProcess = $allDevices | Where-Object { $_.lastUpdate -gt $lastSyncTimestampUnix }
    Write-Log -Message "$($devicesToProcess.Count) of $($allDevices.Count) total devices to process."
    if ($devicesToProcess.Count -eq 0) { Write-Log "No new device updates to process. Sync is complete."; return }

    $existingAssets = Get-FreshAssetsExisting
    $allProducts = Get-FreshProducts
    $existingAssetIndex = Build-ExistingAssetIndex -Assets $existingAssets -RoleToAssetType $roleToAssetType
    $stats = @{ created=0; updated=0; skipped_mapping=0; skipped_id=0; skipped_exist=0; errors=0 }
    
    $i = 0
    foreach ($device in $devicesToProcess) {
        $i++; Write-Progress -Activity "Syncing devices" -Status "Processing $i of $($devicesToProcess.Count)" -PercentComplete (($i / $devicesToProcess.Count) * 100)
        try {
            $role = $device.nodeClass
            if (-not $role -or -not $roleToAssetType.ContainsKey($role)) { $stats.skipped_mapping++; continue }
            $roleConfig = $roleToAssetType[$role]
            if (-not $roleConfig.unique_identifier_mapping) { $stats.skipped_mapping++; continue }
            
            $rawUniqueIdValue = Get-NinjaFieldValue -Device $device -Path $roleConfig.unique_identifier_mapping.ninja_field
            $uniqueIdValue = Normalize-Identifier -Identifier $rawUniqueIdValue
            if (-not $uniqueIdValue) { $stats.skipped_id++; continue }

            $assetTypeId = $roleConfig.asset_type_id
            $key = "$($assetTypeId)|$uniqueIdValue"
            $existingAsset = $existingAssetIndex[$key]
            
            $body = @{ asset_type_id = $assetTypeId }
            $typeFieldsBody = @{}

            $model = $device.system.model
            if ($model -and $roleConfig.PSObject.Properties.Name -contains 'product_field_name' -and $roleConfig.product_field_name) {
                $productId = $null
                if ($modelToProduct.ContainsKey($model)) { $productId = $modelToProduct[$model].product_id }
                else {
                    $existingProd = $allProducts | Where-Object { $_.name -eq $model } | Select-Object -First 1
                    if ($existingProd) {
                        $productId = $existingProd.id; $modelToProduct[$model] = @{ product_id=$existingProd.id; product_name=$existingProd.name }
                    } else {
                        try {
                            Write-Log "Product for model '$model' not found, creating it..." -Level INFO
                            $newProdBody = @{ name=$model; manufacturer=$device.system.manufacturer; asset_type_id=$assetTypeId }
                            $newProdResult = Invoke-FreshApi -Method POST -Path "/products" -Body $newProdBody
                            $createdProd = $newProdResult.product
                            $productId = $createdProd.id
                            $allProducts += $createdProd; $modelToProduct[$model] = @{ product_id=$createdProd.id; product_name=$createdProd.name }
                        } catch { Write-Log "Failed to auto-create product for '$model': $($_.Exception.Message)" -Level ERROR }
                    }
                }
                if ($productId) { $typeFieldsBody[$roleConfig.product_field_name] = $productId }
            }

            foreach ($map in $roleConfig.field_mappings) {
                $rawValue = $null
                if ($map.PSObject.Properties.Name -contains 'static_value') {
                    if (-not $existingAsset) { $rawValue = $map.static_value }
                } else {
                    if ($map.fs_name -eq "name") {
                        $rawValue = $device.systemName
                        if ([string]::IsNullOrWhiteSpace($rawValue)) {
                            $rawValue = "Ninja device $($device.id)"
                        }
                    } else {
                        $rawValue = Get-NinjaFieldValue -Device $device -Path $map.ninja_field
                    }
                }
                if ($rawValue -and $rawValue -ne "") {
                    if ($map.fs_name -match '_\d+$') { $typeFieldsBody[$map.fs_name] = $rawValue }
                    else { $body[$map.fs_name] = $rawValue }
                }
            }

            if ($typeFieldsBody.Count -gt 0) { $body['type_fields'] = $typeFieldsBody }
            
            if ($existingAsset) {
                if (-not $UpdateExisting) { $stats.skipped_exist++; continue }
                if ($DryRun) { $stats.updated++; Write-Log -Message "DRY RUN - Would UPDATE asset id $($existingAsset.id) for Ninja device $($device.id)" -Level WARN; continue }
                Write-Log -Message "DEBUG: Payload for asset update (ID $($existingAsset.id)): $($body | ConvertTo-Json -Depth 10)" -Level DEBUG
                Invoke-FreshApi -Method PUT -Path "/assets/$($existingAsset.id)" -Body @{ asset = $body }
                $stats.updated++; Write-Log -Message "Updated asset id $($existingAsset.id) for Ninja device $($device.id)."
            } else {
                if ($DryRun) { $stats.created++; Write-Log -Message "DRY RUN - Would CREATE asset for Ninja device $($device.id)" -Level WARN; continue }
                Write-Log -Message "DEBUG: Payload for asset creation: $($body | ConvertTo-Json -Depth 10)" -Level DEBUG
                $resp = Invoke-FreshApi -Method POST -Path "/assets" -Body @{ asset = $body }
                $stats.created++; Write-Log -Message "Created asset for $($device.id) -> FS asset id $($resp.asset.id)"
                if ($resp.asset -and -not $existingAssetIndex.ContainsKey($key)) { $existingAssetIndex[$key] = $resp.asset }
            }
        } catch {
            $stats.errors++; Write-Log -Message "Unhandled error for Ninja device ID $($device.id): $($_.ToString())" -Level ERROR
        }
    }
    
    $stats.skipped_total = $stats.skipped_mapping + $stats.skipped_id + $stats.skipped_exist
    Write-Log -Message "Sync complete."
    if ($stats.errors -eq 0) {
        Write-Log -Message "Sync successful. Updating state file."
        @{ lastSuccessfulSyncUtc = $syncStartTime.ToString("o") } | ConvertTo-Json | Set-Content -Path $stateFilePath
    } else { Write-Log "Sync completed with errors. State file will NOT be updated." -Level WARN }
    
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
    $script:DebugMode = $DebugMode.IsPresent

    if (-not [string]::IsNullOrWhiteSpace($NinjaRegion)) {
        Write-Log -Message "Overriding NinjaRegion from config.json with value from parameter: $NinjaRegion" -Level WARN
        $script:Config.NinjaRegion = $NinjaRegion
    }
    if (-not [string]::IsNullOrWhiteSpace($FreshSubdomain)) {
        Write-Log -Message "Overriding FreshSubdomain from config.json with value from parameter: $FreshSubdomain" -Level WARN
        $script:Config.FreshSubdomain = $FreshSubdomain
    }

    $script:SecretManagementAvailable = [bool](Get-Command Get-Secret -ErrorAction SilentlyContinue)
    if ($script:SecretManagementAvailable) { Write-Log "PowerShell SecretManagement module detected." -Level DEBUG }

    $ninjaClientId = Resolve-Credential -ParamValue $NinjaClientId -ConfigKeyName "NinjaClientId" -SecretName $script:Config.Secrets.NinjaClientId -EnvVarName "NINJAONE_CLIENT_ID"
    $ninjaClientSecret = Resolve-Credential -ParamValue $NinjaClientSecret -ConfigKeyName "NinjaClientSecret" -SecretName $script:Config.Secrets.NinjaClientSecret -EnvVarName "NINJAONE_CLIENT_SECRET"
    $script:FreshApiKey = Resolve-Credential -ParamValue $FreshApiKey -ConfigKeyName "FreshApiKey" -SecretName $script:Config.Secrets.FreshApiKey -EnvVarName "FRESHSERVICE_API_KEY"
    
    $missingCredentials = New-Object System.Collections.Generic.List[string]
    if (-not $ninjaClientId) { $missingCredentials.Add("NinjaClientId") }
    if (-not $ninjaClientSecret) { $missingCredentials.Add("NinjaClientSecret") }
    if (-not $script:FreshApiKey) { $missingCredentials.Add("FreshApiKey") }

    if ($missingCredentials.Count -gt 0) {
        $missingList = $missingCredentials -join ", "
        $errorMessage = "Could not resolve the following required credentials: $missingList. " +
                        "Please provide them using command-line parameters (e.g., -NinjaClientId '...'), " +
                        "a configured PowerShell secret vault, or environment variables (e.g., `$env:NINJAONE_CLIENT_ID`)."
        
        # Log the error and exit directly to prevent the script from continuing or throwing
        Write-Log -Message $errorMessage -Level ERROR
        # Send a notification if possible
        if ($script:Config.Webhook.Uri) {
            Send-WebhookNotification -Title "Freshservice Sync FAILED" -Message $errorMessage -Color "danger"
        }
        # Exit with a non-zero status code
        exit 1
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
