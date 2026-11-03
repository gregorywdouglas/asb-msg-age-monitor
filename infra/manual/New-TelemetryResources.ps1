<#
.SYNOPSIS
    Creates the telemetry resources from infra/modules/telemetry.bicep without an ARM
    deployment: the two custom tables, the Data Collection Endpoint and the Data
    Collection Rule.

.DESCRIPTION
    For principals that may create these resources but may not run template deployments
    (Microsoft.Resources/deployments/write). Each resource is PUT directly with `az rest`,
    which needs exactly the permissions the portal needs.

    Column definitions are read from infra/schema/*.json — the same files the Bicep module
    and TST-060c use — so the table schema and the DCR stream declaration cannot drift
    from the emitted rows. A column the function sends that the DCR does not declare is
    dropped silently (spec §8.2).

    The hourly rollup table and summary rule (spec §8.4) are not created here; ingestion
    does not depend on them.

    The *-sample.json and *-transform.kql files beside this script are for the portal's
    "New custom log (DCR-based)" wizard, the route when neither this script nor Bicep can
    be run. Upload the sample to define the table and paste the transform. Both mirror
    infra/schema/*.json column for column; the sample values are chosen so the wizard
    infers each type correctly (3000000000 forces long). A test keeps them in step with
    the schema.

    Idempotent: re-running updates the resources in place.

.EXAMPLE
    ./infra/manual/New-TelemetryResources.ps1 -ResourceGroup rg-dev-eie-core -WorkspaceName log-dev-eie -EnvironmentName dev -GrantSignedInUser
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $ResourceGroup,
    [Parameter(Mandatory)] [string] $WorkspaceName,
    [Parameter(Mandatory)] [ValidateSet('dev', 'staging', 'prod')] [string] $EnvironmentName,
    [string] $SubscriptionId,
    [int] $InteractiveRetentionDays = 90,
    [int] $TotalRetentionDays = 730,

    # Object ids of managed identities or service principals to grant Monitoring Metrics
    # Publisher on the DCR. Use -GrantSignedInUser for yourself.
    [string[]] $PublisherObjectIds = @(),

    # Also grant the signed-in az user, so a local debug session can ingest.
    [switch] $GrantSignedInUser
)

$ErrorActionPreference = 'Stop'
$schemaDir = Join-Path $PSScriptRoot '..\schema'
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

function Invoke-Az {
    $output = & az @args
    if ($LASTEXITCODE -ne 0) { throw "az $($args -join ' ') failed (exit $LASTEXITCODE)." }
    return $output
}

function Invoke-ArmRest([string] $Method, [string] $Url, $Body) {
    $azArgs = @('rest', '--method', $Method, '--url', $Url)
    $file = $null
    if ($null -ne $Body) {
        # Body goes through a BOM-less file: inline JSON does not survive PowerShell's
        # argument quoting, and a BOM is rejected by the ARM endpoint.
        $file = [System.IO.Path]::GetTempFileName()
        [System.IO.File]::WriteAllText($file, (ConvertTo-Json $Body -Depth 20), $utf8NoBom)
        $azArgs += @('--body', "@$file", '--headers', 'Content-Type=application/json')
    }
    try {
        $json = Invoke-Az @azArgs
        if ($json) { return ($json -join "`n") | ConvertFrom-Json }
    }
    finally {
        if ($file) { Remove-Item $file -ErrorAction SilentlyContinue }
    }
}

function Read-Columns([string] $FileName) {
    # Re-wrap as plain hashtables so ConvertTo-Json emits an array even for PowerShell 5.1.
    $columns = Get-Content -Raw (Join-Path $schemaDir $FileName) | ConvertFrom-Json
    return , @($columns | ForEach-Object { @{ name = $_.name; type = $_.type } })
}

if (-not $SubscriptionId) { $SubscriptionId = Invoke-Az account show --query id -o tsv }

$arm = 'https://management.azure.com'
$rgPath = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup"
$workspacePath = "$rgPath/providers/Microsoft.OperationalInsights/workspaces/$WorkspaceName"

$measurementTable = 'ServiceBusMessageAge_CL'
$healthTable = 'ServiceBusMonitorHealth_CL'
$dceName = "dce-asbmon-$EnvironmentName"
$dcrName = "dcr-asbmon-$EnvironmentName"

# The DCR must be in the workspace's region, and the DCE in the DCR's.
$workspace = Invoke-ArmRest GET "$arm$workspacePath`?api-version=2023-09-01"
$location = $workspace.location
Write-Host "Workspace $WorkspaceName is in '$location'; DCE and DCR will be created there."

# --- Custom tables ------------------------------------------------------------------
$tables = @(
    @{ Name = $measurementTable; Columns = Read-Columns 'measurement-columns.json' },
    @{ Name = $healthTable; Columns = Read-Columns 'health-columns.json' }
)

foreach ($table in $tables) {
    Write-Host "Creating table $($table.Name)..."
    $tableUrl = "$arm$workspacePath/tables/$($table.Name)?api-version=2023-09-01"
    Invoke-ArmRest PUT $tableUrl @{
        properties = @{
            plan                 = 'Analytics'
            retentionInDays      = $InteractiveRetentionDays
            totalRetentionInDays = $TotalRetentionDays
            schema               = @{ name = $table.Name; columns = $table.Columns }
        }
    } | Out-Null

    # Table creation is asynchronous, and the DCR PUT rejects an output stream whose
    # table is not yet provisioned.
    for ($i = 0; $i -lt 30; $i++) {
        $state = (Invoke-ArmRest GET $tableUrl).properties.provisioningState
        if ($state -eq 'Succeeded') { break }
        Start-Sleep -Seconds 5
    }
    if ($state -ne 'Succeeded') { throw "Table $($table.Name) is still '$state' after 150 s." }
}

# --- Data Collection Endpoint -------------------------------------------------------
Write-Host "Creating DCE $dceName..."
$dce = Invoke-ArmRest PUT "$arm$rgPath/providers/Microsoft.Insights/dataCollectionEndpoints/$dceName`?api-version=2023-03-11" @{
    location   = $location
    properties = @{
        # Spec §10.2 / OPN-004: public with firewall restriction is the decided posture.
        networkAcls = @{ publicNetworkAccess = 'Enabled' }
    }
}

# --- Data Collection Rule -----------------------------------------------------------
Write-Host "Creating DCR $dcrName..."
$streamDeclarations = @{}
$dataFlows = @()
foreach ($table in $tables) {
    $stream = "Custom-$($table.Name)"
    $streamDeclarations[$stream] = @{ columns = $table.Columns }
    $dataFlows += @{
        streams      = @($stream)
        destinations = @('law')
        transformKql = 'source'
        outputStream = $stream
    }
}

$dcr = Invoke-ArmRest PUT "$arm$rgPath/providers/Microsoft.Insights/dataCollectionRules/$dcrName`?api-version=2023-03-11" @{
    location   = $location
    properties = @{
        dataCollectionEndpointId = $dce.id
        streamDeclarations       = $streamDeclarations
        destinations             = @{
            logAnalytics = @(@{ workspaceResourceId = $workspace.id; name = 'law' })
        }
        dataFlows                = $dataFlows
    }
}

# --- Monitoring Metrics Publisher on the DCR ----------------------------------------
$publishers = @($PublisherObjectIds | ForEach-Object { @{ Id = $_; Type = 'ServicePrincipal' } })
if ($GrantSignedInUser) {
    $publishers += @{ Id = (Invoke-Az ad signed-in-user show --query id -o tsv); Type = 'User' }
}
foreach ($publisher in $publishers) {
    Write-Host "Granting Monitoring Metrics Publisher on $dcrName to $($publisher.Id)..."
    # --assignee would resolve the id through Microsoft Graph first, which fails where the
    # caller cannot read the directory and for a managed identity not yet replicated.
    # List filters by principalId client-side for the same reason.
    $existing = Invoke-Az role assignment list --role 'Monitoring Metrics Publisher' --scope $dcr.id `
        --query "[?principalId=='$($publisher.Id)'].id" -o tsv
    if (-not $existing) {
        Invoke-Az role assignment create --role 'Monitoring Metrics Publisher' --scope $dcr.id `
            --assignee-object-id $publisher.Id --assignee-principal-type $publisher.Type | Out-Null
    }
}

Write-Host ''
Write-Host 'Settings for the function (local.settings.json uses ":", Azure app settings use "__"):'
Write-Host "  Telemetry:DceEndpoint   = $($dce.properties.logsIngestion.endpoint)"
Write-Host "  Telemetry:DcrImmutableId = $($dcr.properties.immutableId)"
Write-Host ''
Write-Host 'Not created: the hourly rollup table ServiceBusMessageAgeHourly_CL and its summary rule (spec §8.4).'
