@echo off
REM ============================================================
REM Installation - Archivportal-D Scraper
REM Compatible: Windows 10/11
REM ============================================================

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "VENV_DIR=%SCRIPT_DIR%venv"

echo ============================================================
echo   INSTALLATION
echo ============================================================
echo.

REM Verifier Python
echo [1/4] Verification de Python...

where python >nul 2>&1
if %errorlevel% neq 0 (
    echo ERREUR: Python n'est pas installe.
    echo Installez Python 3.9+ depuis https://www.python.org/downloads/
    echo IMPORTANT: Cochez "Add Python to PATH" lors de l'installation
    pause
    exit /b 1
)

for /f "tokens=*" %%i in ('python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"') do set PYTHON_VERSION=%%i
echo       Python %PYTHON_VERSION% detecte

REM Creer l'environnement virtuel
echo.
echo [2/4] Creation de l'environnement virtuel...

if exist "%VENV_DIR%" (
    echo       Environnement existant detecte, suppression...
    rmdir /s /q "%VENV_DIR%"
)

python -m venv "%VENV_DIR%"
if %errorlevel% neq 0 (
    echo ERREUR: Impossible de creer l'environnement virtuel
    pause
    exit /b 1
)
echo       Environnement cree dans: %VENV_DIR%

REM Activer et installer
echo.
echo [3/4] Installation des dependances...

call "%VENV_DIR%\Scripts\activate.bat"

REM Mise a jour pip
python -m pip install --upgrade pip --quiet

REM Installation des dependances
pip install -r "%SCRIPT_DIR%requirements.txt" --quiet

if %errorlevel% neq 0 (
    echo ERREUR: Echec de l'installation des dependances
    pause
    exit /b 1
)

echo       Dependances installees avec succes

REM Creer le script de lancement
echo.
echo [4/4] Creation du script de lancement...

(
echo @echo off
echo setlocal
echo set "SCRIPT_DIR=%%~dp0"
echo call "%%SCRIPT_DIR%%venv\Scripts\activate.bat"
echo python "%%SCRIPT_DIR%%scraper.py" %%*
echo endlocal
) > "%SCRIPT_DIR%run.bat"

echo.
echo ============================================================
echo   INSTALLATION TERMINEE
echo ============================================================
echo.
echo   Pour lancer le scraper:
echo.
echo     run.bat                     Mode rapide
echo     run.bat --detailed          Mode detaille (plus precis)
echo     run.bat --limit 50          Test avec 50 resultats
echo     run.bat --help              Aide complete
echo.
echo   Les resultats seront dans le dossier 'output/'
echo.
echo ============================================================
echo.
pause
