<#
.SYNOPSIS
    Publishes the Service Bus entity catalog to App Configuration, where the monitor
    reads it instead of calling ARM (SPEC-DEVIATION-008).

.DESCRIPTION
    The function cannot reach the ARM management plane, so it cannot enumerate entities
    itself. The deployment pipeline can, so it enumerates them here and writes one key
    set per entity under asbmon:catalog:entities:<entityPath>:*, then bumps the sentinel
    so running instances reload within AppConfig:RefreshIntervalSeconds.

    Every catalog key is labelled with the namespace name, because the store may be
    shared by monitors of other namespaces. Entity paths are namespace-relative, so
    without the label two namespaces would overwrite each other's entries, and the
    strict import of one would delete the other's whole catalog. The function loads
    only the label matching its own ServiceBus:NamespaceName.

    Run it:
      - as the last step of every pipeline that creates, changes or deletes queues,
        topics or subscriptions in the namespace, and
      - on a schedule (hourly is enough) as a backstop for a change made elsewhere.
    An entity missing from the catalog is not measured, and nothing alerts on that.

    The import is strict within this namespace's label: catalog keys for entities that
    no longer exist are deleted,
    which is how the monitor learns of a deletion (FR-005). An enumeration that finds no
    entities is refused rather than published, so a failed listing cannot wipe the
    catalog.

    Pipeline identity needs:
      - Reader on the namespace (ARM listing)
      - App Configuration Data Owner on the store (data-plane write)

.EXAMPLE
    ./infra/pipeline/Publish-EntityCatalog.ps1 -ServiceBusResourceGroup rg-dev-eie-core -ServiceBusNamespaceName sb-dev-eie-core -AppConfigEndpoint https://appcs-dev-eie.azconfig.io
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $ServiceBusResourceGroup,
    [Parameter(Mandatory)] [string] $ServiceBusNamespaceName,
    [Parameter(Mandatory)] [string] $AppConfigEndpoint,
    [string] $SubscriptionId,
    [string] $SentinelKey = 'asbmon:sentinel',

    # Write the catalog file and print it without touching App Configuration.
    [switch] $DryRun
)

$ErrorActionPreference = 'Stop'
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$prefix = 'asbmon:catalog:'

function Invoke-Az {
    $output = & az @args
    if ($LASTEXITCODE -ne 0) { throw "az $($args -join ' ') failed (exit $LASTEXITCODE)." }
    return $output
}

function Invoke-AzJson {
    $json = Invoke-Az @args -o json
    if (-not $json) { return @() }
    # Windows PowerShell 5.1 emits a JSON array as one object; piping unrolls it.
    $parsed = ($json -join "`n") | ConvertFrom-Json
    return @($parsed | ForEach-Object { $_ })
}

# az reports TTLs as ISO 8601 durations. The monitor accepts both forms, but the
# invariant TimeSpan form is unambiguous to a human reading the store.
function ConvertTo-TimeSpanText([string] $Value) {
    if ([string]::IsNullOrWhiteSpace($Value)) { return $null }
    if ($Value.StartsWith('P')) {
        return [System.Xml.XmlConvert]::ToTimeSpan($Value).ToString('c', [System.Globalization.CultureInfo]::InvariantCulture)
    }
    return $Value
}

# Every value is written as a string: App Configuration values are strings, and the
# monitor parses them invariantly.
function New-Entry([string] $Kind, $Entity, [bool] $Partitioned) {
    $entry = [ordered]@{
        kind                     = $Kind
        enablePartitioning       = $Partitioned.ToString().ToLowerInvariant()
        maxDeliveryCount         = [string]$Entity.maxDeliveryCount
        defaultMessageTimeToLive = ConvertTo-TimeSpanText $Entity.defaultMessageTimeToLive
    }
    if (-not [string]::IsNullOrWhiteSpace($Entity.forwardTo)) { $entry.forwardTo = $Entity.forwardTo }
    if (-not $entry.defaultMessageTimeToLive) { $entry.Remove('defaultMessageTimeToLive') }
    return $entry
}

$scope = @('--resource-group', $ServiceBusResourceGroup, '--namespace-name', $ServiceBusNamespaceName)
if ($SubscriptionId) { $scope += @('--subscription', $SubscriptionId) }

# --- Enumerate (ARM) ----------------------------------------------------------------
$entities = [ordered]@{}

foreach ($queue in Invoke-AzJson servicebus queue list @scope) {
    $entities[$queue.name] = New-Entry 'Queue' $queue ([bool]$queue.enablePartitioning)
}

foreach ($topic in Invoke-AzJson servicebus topic list @scope) {
    # Partitioning is a topic property; its subscriptions inherit it (FR-072).
    $partitioned = [bool]$topic.enablePartitioning
    foreach ($subscription in Invoke-AzJson servicebus topic subscription list @scope --topic-name $topic.name) {
        $entities["$($topic.name)/Subscriptions/$($subscription.name)"] = New-Entry 'Subscription' $subscription $partitioned
    }
}

if ($entities.Count -eq 0) {
    throw "No queues or subscriptions found in $ServiceBusNamespaceName. Refusing to publish an empty catalog: the strict import would delete every catalog entry."
}

$catalog = [ordered]@{
    publishedAtUtc = [DateTime]::UtcNow.ToString('o', [System.Globalization.CultureInfo]::InvariantCulture)
    entities       = $entities
}

$file = [System.IO.Path]::GetTempFileName()
[System.IO.File]::WriteAllText($file, (ConvertTo-Json $catalog -Depth 10), $utf8NoBom)
Write-Host "Enumerated $($entities.Count) entities from $ServiceBusNamespaceName."

if ($DryRun) {
    Get-Content $file
    Remove-Item $file
    return
}

# --- Publish (App Configuration data plane) -----------------------------------------
try {
    # --strict deletes only keys matching both the prefix and this label, so other
    # namespaces' catalogs in a shared store are untouched.
    Invoke-Az appconfig kv import `
        --endpoint $AppConfigEndpoint --auth-mode login `
        --source file --path $file --format json `
        --separator ':' --prefix $prefix --label $ServiceBusNamespaceName `
        --strict --yes | Out-Null

    # Catalogs published before labelling carried no label. The function loads all
    # unlabelled asbmon:* keys, so any left behind would keep feeding it entities from
    # whichever namespace published last. Nothing writes unlabelled catalog keys now.
    Invoke-Az appconfig kv delete `
        --endpoint $AppConfigEndpoint --auth-mode login `
        --key "$prefix*" --label '\0' --yes | Out-Null
}
finally {
    Remove-Item $file -ErrorAction SilentlyContinue
}

# The monitor reloads everything when the sentinel changes. Bumped last, so an instance
# never reloads a half-written catalog.
Invoke-Az appconfig kv set `
    --endpoint $AppConfigEndpoint --auth-mode login `
    --key $SentinelKey --value $catalog.publishedAtUtc --yes | Out-Null

Write-Host "Published catalog under '$prefix' with label '$ServiceBusNamespaceName' and bumped $SentinelKey."
