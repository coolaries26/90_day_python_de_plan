param(
    [int]$Day,
    [string]$Message = "Daily progress update"
)

if (-not $Day) {
    Write-Host "Usage: `$0 -Day N -Message 'message'"
    exit 1
}

$branch = git branch --show-current
$dayNum = $("{0:D3}" -f $Day)
$commitMsg = "[DAY-$dayNum] $Message"

Write-Host "Branch:  $branch"
Write-Host "Commit:  $commitMsg"
Write-Host ""

git add .
try {
    git commit -m "$commitMsg"
} catch {
    Write-Host "Nothing to commit"
}

try {
    git push -u origin $branch
    Write-Host "Pushed to $branch"
} catch {
    Write-Host "Push failed"
}
