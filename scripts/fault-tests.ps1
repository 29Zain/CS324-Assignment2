# ---------------------------------------------------------------------------
# Automated fault-tolerance tests for DistriLab (Windows PowerShell).
# Starts a Bootstrap Node and 5 workers in the background, runs jobs, crashes a
# worker and the coordinator in the middle of jobs, adds a worker mid-job and
# exercises the 5-job term limit. Every result is checked against the known answer.
# Usage: scripts\fault-tests.bat   (build first; takes ~5 minutes)
# Logs of every process are written to logs\fault-tests\
# ---------------------------------------------------------------------------
$ErrorActionPreference = "Continue"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$logs = Join-Path $root "logs\fault-tests"
Remove-Item -Recurse -Force $logs -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force $logs | Out-Null
Set-Location $root
$jar  = "target\distrilab.jar"
$boot = "-cp $jar bootstrap.BootstrapNode"
$work = "-cp $jar worker.Worker"
$cli  = "-cp $jar client.Client"
$global:procs = @{}

function Log($m) { $line = "{0:HH:mm:ss}  {1}" -f (Get-Date), $m; Write-Output $line }

function StartWorker($id, $extra) {
  $argLine = "$work --bootstrap=localhost:50050 --id=$id --port=$(50050 + $id) --minWorkersForFirstElection=5 $extra"
  $p = Start-Process java -ArgumentList $argLine -WindowStyle Hidden -PassThru `
       -RedirectStandardOutput "$logs\worker$id.out" -RedirectStandardError "$logs\worker$id.log"
  $global:procs["w$id"] = $p
  Log "started Worker-$id (pid $($p.Id))"
}

function Status() { $o = & java -cp $jar client.Client --status 2>$null | Out-String; return $o }

function CoordinatorId() {
  $s = Status
  if ($s -match "Coordinator: Worker-(\d+)") { return [int]$Matches[1] }
  return -1
}

function RunJob($name, $jobArgs) {
  $out = "$logs\job-$name.txt"
  $p = Start-Process java -ArgumentList "$cli --cli $jobArgs" -WindowStyle Hidden -PassThru `
       -RedirectStandardOutput $out -RedirectStandardError "$logs\job-$name.err"
  return @{ proc = $p; out = $out; name = $name }
}

function WaitAccepted($job) {
  # Wait (max 60 s) until the coordinator has accepted the job, so the crash really happens mid-job.
  for ($i = 0; $i -lt 120; $i++) {
    if ((Test-Path $job.out) -and (Select-String -Path $job.out -Pattern "Accepted by" -Quiet)) { return }
    Start-Sleep -Milliseconds 500
  }
}

function Finish($job, $expected) {
  $job.proc.WaitForExit(600000) | Out-Null
  $text = Get-Content $job.out -Raw
  $finals = ($text -split "`n") | Where-Object { $_ -match "\] (COMPLETED|FAILED|CANCELLED) in" }
  foreach ($f in $finals) {
    $ok = $f -match [regex]::Escape("-> $expected")
    Log ("  [{0}] {1}   => {2}" -f $job.name, $f.Trim(), $(if ($ok) { "PASS" } else { "FAIL (expected $expected)" }))
  }
  if (-not $finals) { Log "  [$($job.name)] FAIL - no result. Output:"; Write-Output $text }
}

function DistriLabProcs($pattern) {
  Get-CimInstance Win32_Process -Filter "Name='java.exe' OR Name='javaw.exe'" |
    Where-Object { $_.CommandLine -like $pattern }
}
function KillMatching($pattern) {
  DistriLabProcs $pattern | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
}
function KillWorker($id) { KillMatching "*worker.Worker*--id=$id *" }

Log "Cleaning up DistriLab processes from earlier runs"
KillMatching "*distrilab.jar*"
Start-Sleep 2
Log "=== Starting Bootstrap Node and 5 workers (one by one) ==="
$global:procs["boot"] = Start-Process java -ArgumentList "$boot --port=50050" -WindowStyle Hidden -PassThru `
       -RedirectStandardOutput "$logs\bootstrap.out" -RedirectStandardError "$logs\bootstrap.log"
Start-Sleep 2
1..5 | ForEach-Object { StartWorker $_ ""; Start-Sleep 1 }
Start-Sleep 9
Log "Cluster status:"; Write-Output (Status)

Log "=== Scenario 1: PRIMESUM 1..1,000,000,000 ==="
$t = Get-Date; $j = RunJob "primesum-1e9" "--type=PRIMESUM --start=1 --end=1000000000"
Finish $j "24739512092254535"; Log ("  took {0:N1} s" -f ((Get-Date) - $t).TotalSeconds)

Log "=== Scenario 2: two clients at the same time, each PRIMESUM 1..1e9 ==="
$a = RunJob "client-A" "--type=PRIMESUM --start=1 --end=1000000000"
$b = RunJob "client-B" "--type=PRIMESUM --start=1 --end=1000000000"
Finish $a "24739512092254535"; Finish $b "24739512092254535"

Log "=== Scenario 3: MAX and PRIMECOUNT on CSV files ==="
Finish (RunJob "max-100k" "--type=MAX --file=data\numbers_100k.csv") "999991518681"
Finish (RunJob "primecount-100k" "--type=PRIMECOUNT --file=data\numbers_100k.csv") "3674"
Finish (RunJob "primecount-large" "--type=PRIMECOUNT --file=data\numbers_large_values_5k.csv") "103"

Log "=== Scenario 4: kill a NON-coordinator worker in the middle of PRIMESUM 1..3e9 ==="
$c = CoordinatorId; Log "  coordinator is Worker-$c"
$victim = (1..5 | Where-Object { $_ -ne $c } | Select-Object -First 1)
$j = RunJob "kill-worker" "--type=PRIMESUM --start=1 --end=3000000000"
WaitAccepted $j; Start-Sleep 3
Log "  killing Worker-$victim (simulated crash)"; KillWorker $victim
Finish $j "211169315361937456"
Start-Sleep 3; Write-Output (Status)

Log "=== Scenario 5: kill the COORDINATOR in the middle of PRIMESUM 1..3e9 ==="
$c = CoordinatorId; Log "  coordinator is Worker-$c"
$j = RunJob "kill-coordinator" "--type=PRIMESUM --start=1 --end=3000000000"
WaitAccepted $j; Start-Sleep 3
Log "  killing coordinator Worker-$c (simulated crash)"; KillWorker $c
Finish $j "211169315361937456"
Get-Content $j.out | Select-String "Lost contact|owned by|Accepted" | ForEach-Object { Log ("  client: " + $_.Line.Trim()) }
Start-Sleep 2; Write-Output (Status)

Log "=== Scenario 6: a NEW worker joins while a job is running ==="
$j = RunJob "join-mid-job" "--type=PRIMESUM --start=1 --end=3000000000"
Start-Sleep 2
$p = Start-Process java -ArgumentList "$work --bootstrap=localhost:50050" -WindowStyle Hidden -PassThru `
     -RedirectStandardOutput "$logs\worker-new.out" -RedirectStandardError "$logs\worker-new.log"
$global:procs["wnew"] = $p
Finish $j "211169315361937456"
Get-Content "$logs\worker-new.log" | Select-String "started at|Running subtask" | Select-Object -First 4 | ForEach-Object { Log ("  new worker: " + $_.Line.Trim()) }

Log "=== Scenario 7: 6 jobs -> 5-job term limit and re-election ==="
$j = RunJob "term-limit" "--type=PRIMESUM --start=1 --end=20000000 --copies=6"
$j.proc.WaitForExit(600000) | Out-Null
Get-Content $j.out | Select-String "COMPLETED in|FAILED in|Accepted by" | ForEach-Object { Log ("  " + $_.Line.Trim()) }
Write-Output (Status)

Log "=== Election / failure lines from worker logs ==="
Get-ChildItem "$logs\worker*.log" | ForEach-Object {
  $n = $_.Name
  Get-Content $_.FullName | Select-String "COORDINATOR AGREED|FAILURE DETECTED|RECOVERED|stepping down|finished:.*winner|-> DEAD|reassigned" |
    ForEach-Object { Write-Output ("[$n] " + $_.Line.Trim()) }
}

Log "=== Stopping everything ==="
KillMatching "*distrilab.jar*"
Log "DONE"
