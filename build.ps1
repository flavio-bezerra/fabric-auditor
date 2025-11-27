<#
.SYNOPSIS
    Script automático para compilar pacotes Python em .whl
.DESCRIPTION
    1. Verifica se está na pasta correta (procura setup.py).
    2. Verifica se o módulo 'build' está instalado no Python.
    3. Instala 'build' se necessário.
    4. Limpa builds antigos (pastas dist/build).
    5. Gera o novo .whl.
.AUTHOR
    Flavio Bezerra (Assistente IA)
#>

# 1. Define o local do script como diretório de trabalho
$scriptPath = $PSScriptRoot
Set-Location -Path $scriptPath

Write-Host "------------------------------------------------------" -ForegroundColor Cyan
Write-Host "   FABRIC AUDITOR - BUILD AUTOMATION TOOL" -ForegroundColor Cyan
Write-Host "------------------------------------------------------" -ForegroundColor Cyan
Write-Host "Diretório de trabalho: $scriptPath" -ForegroundColor Gray

# 2. Verifica se o setup.py existe
if (-not (Test-Path "$scriptPath\setup.py") -and -not (Test-Path "$scriptPath\pyproject.toml")) {
    Write-Host "ERRO: Não encontrei 'setup.py' ou 'pyproject.toml' nesta pasta." -ForegroundColor Red
    Write-Host "Certifique-se de colocar este script na RAIZ do projeto." -ForegroundColor Yellow
    Read-Host -Prompt "Pressione ENTER para sair"
    exit
}

# 3. Verifica se o Python está acessível
try {
    $pythonVersion = python --version 2>&1
    Write-Host "Python detectado: $pythonVersion" -ForegroundColor Green
}
catch {
    Write-Host "ERRO: Python não encontrado no PATH do sistema." -ForegroundColor Red
    Read-Host -Prompt "Pressione ENTER para sair"
    exit
}

# 4. Verifica e instala o módulo 'build'
Write-Host "`nVerificando dependências de build..." -ForegroundColor Yellow
python -c "import build" 2>$null

if ($LASTEXITCODE -ne 0) {
    Write-Host "O módulo 'build' não foi encontrado. Instalando agora..." -ForegroundColor Magenta
    pip install build
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERRO CRÍTICO: Falha ao instalar o pacote 'build'." -ForegroundColor Red
        Read-Host -Prompt "Pressione ENTER para sair"
        exit
    }
} else {
    Write-Host "Módulo 'build' já está instalado." -ForegroundColor Green
}

# 5. Limpa builds anteriores para evitar confusão
if (Test-Path "$scriptPath\dist") {
    Write-Host "Limpando pasta 'dist' antiga..." -ForegroundColor Gray
    Remove-Item "$scriptPath\dist" -Recurse -Force
}
if (Test-Path "$scriptPath\build") {
    Write-Host "Limpando pasta 'build' antiga..." -ForegroundColor Gray
    Remove-Item "$scriptPath\build" -Recurse -Force
}
if (Test-Path "$scriptPath\*.egg-info") {
    Write-Host "Limpando egg-info antigo..." -ForegroundColor Gray
    Remove-Item "$scriptPath\*.egg-info" -Recurse -Force
}

# 6. Executa o build
Write-Host "`nIniciando compilação do pacote..." -ForegroundColor Cyan
Write-Host "------------------------------------------------------" -ForegroundColor Gray

python -m build

Write-Host "------------------------------------------------------" -ForegroundColor Gray

# 7. Verifica o resultado
if ($LASTEXITCODE -eq 0) {
    $whlFile = Get-ChildItem "$scriptPath\dist\*.whl" | Select-Object -First 1
    
    Write-Host "`nSUCESSO! O build foi concluído." -ForegroundColor Green
    Write-Host "Arquivo gerado:" -ForegroundColor White
    Write-Host "  $($whlFile.FullName)" -ForegroundColor Cyan
    
    # Abre a pasta dist automaticamente para facilitar
    Invoke-Item "$scriptPath\dist"
} else {
    Write-Host "`nFALHA no processo de build. Verifique os erros acima." -ForegroundColor Red
}

Write-Host "`n"
Read-Host -Prompt "Pressione ENTER para fechar"