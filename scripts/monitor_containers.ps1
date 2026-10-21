$taskLimit = 240
New-Item -ItemType Directory -Force -Path reports | Out-Null
for ($taskIndex=0; $taskIndex -lt $taskLimit; $taskIndex++) {
    if (Test-Path -LiteralPath runtime/stop-monitor) { break }
    $taskContainers = @(docker ps --filter 'label=com.docker.compose.project=banking-distributed-ml' --format '{{.ID}}')
    if ($taskContainers.Count -eq 0) { break }
    $taskStats = docker stats --no-stream --format '{{json .}}' @taskContainers
    foreach ($taskRow in $taskStats) {
        @{ time=(Get-Date).ToUniversalTime().ToString('o'); stats=($taskRow | ConvertFrom-Json) } | ConvertTo-Json -Compress | Add-Content -LiteralPath reports/docker-stats.jsonl
    }
    Start-Sleep -Seconds 3
}
