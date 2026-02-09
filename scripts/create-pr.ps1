# create-pr.ps1 — Автоматизация создания Pull Request
# Использование: .\scripts\create-pr.ps1 [-Title "Заголовок"] [-Body "Описание"]
# Из папки репозитория: powershell -ExecutionPolicy Bypass -File scripts/create-pr.ps1

param(
    [string]$Title = "",
    [string]$Body = "",
    [string]$Base = "main"
)

$ErrorActionPreference = "Continue"

# Проверка: находимся в git-репозитории
if (-not (Test-Path ".git")) {
    Write-Error "Запустите скрипт из корня репозитория: pwsh -File scripts/create-pr.ps1"
    exit 1
}

$currentBranch = git rev-parse --abbrev-ref HEAD 2>$null
if (-not $currentBranch) {
    Write-Error "Не удалось определить текущую ветку"
    exit 1
}

if ($currentBranch -eq $Base) {
    Write-Warning "Вы на ветке $Base. Создайте feature-ветку и внесите изменения."
    exit 1
}

# Проверка несохранённых изменений (только предупреждение)
$status = git status --porcelain 2>$null
if ($status) {
    Write-Warning "Есть несохранённые изменения. Push отправит только уже закоммиченное."
    Write-Host "  Чтобы добавить: git add . && git commit -m `"описание`""
    Write-Host ""
}

# Push ветки
Write-Host "Push ветки $currentBranch..." -ForegroundColor Cyan
& git push -u origin $currentBranch 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Warning "Push: ошибка или уже актуально. Продолжаем..."
}

# Получить URL репозитория
$remoteUrl = git remote get-url origin 2>$null
if ($remoteUrl -match "github\.com[:/]([^/]+)/([^/\.]+)") {
    $owner = $Matches[1]
    $repo = $Matches[2] -replace "\.git$", ""
} else {
    Write-Error "Не удалось определить owner/repo из remote origin"
    exit 1
}

$prUrl = "https://github.com/$owner/$repo/compare/$Base...$currentBranch?expand=1"

# Пробуем GitHub CLI
$ghPath = Get-Command gh -ErrorAction SilentlyContinue
if ($ghPath) {
    Write-Host "Создание PR через GitHub CLI..." -ForegroundColor Cyan
    $prArgs = @("pr", "create", "--base", $Base, "--head", $currentBranch)
    if ($Title) { $prArgs += "--title"; $prArgs += $Title }
    if ($Body)  { $prArgs += "--body";  $prArgs += $Body }
    & gh @prArgs
    if ($LASTEXITCODE -eq 0) {
        Write-Host "PR создан успешно." -ForegroundColor Green
        exit 0
    }
}

# Fallback: открыть страницу в браузере
Write-Host "GitHub CLI не найден. Открываю страницу создания PR в браузере..." -ForegroundColor Yellow
Start-Process $prUrl
Write-Host "URL: $prUrl" -ForegroundColor Gray
Write-Host ""
Write-Host "Чтобы создать PR из терминала, установите GitHub CLI:" -ForegroundColor Yellow
Write-Host "  winget install GitHub.cli" -ForegroundColor Gray
