@echo off
setlocal enabledelayedexpansion

:: ==============================================================================
:: MEDALLION ANALYTICS PLATFORM - Script de Inicio Automático para Windows
:: Proyecto: Pipeline-analitica
:: Autor: Jefri Calderin (Ingeniero de Ingesta) & Equipo Medallion
:: ==============================================================================

title Plataforma Medallion - Control del Proyecto
cd /d "%~dp0"

:: Manejo de argumentos CLI
set "ACTION=%~1"
if "%ACTION%"=="" goto :show_menu

if /i "%ACTION%"=="menu" goto :show_menu
if /i "%ACTION%"=="all" goto :start_all
if /i "%ACTION%"=="app" goto :start_app
if /i "%ACTION%"=="docker" goto :start_docker
if /i "%ACTION%"=="stop" goto :stop_all
if /i "%ACTION%"=="test" goto :run_tests
if /i "%ACTION%"=="mock" goto :gen_mock
if /i "%ACTION%"=="help" goto :show_help
if /i "%ACTION%"=="-h" goto :show_help
if /i "%ACTION%"=="--help" goto :show_help

echo [ERROR] Comando no reconocido: %ACTION%
echo.
goto :show_help

:: ==============================================================================
:: MENÚ INTERACTIVO (al hacer doble clic o ejecutar sin parámetros)
:: ==============================================================================
:show_menu
cls
echo ===============================================================================
echo             PLATAFORMA ANALITICA MEDALLION - PANEL DE CONTROL
echo ===============================================================================
echo.
echo   [1] Iniciar Todo (Docker Desktop + MinIO/Airflow/Spark + FastAPI + Streamlit)
echo   [2] Iniciar Solo Aplicacion Web (FastAPI :8000 + Streamlit :8501 en modo local)
echo   [3] Iniciar Solo Stack Docker (Abre Docker Desktop e inicia contenedores)
echo   [4] Ejecutar Suite Completa de Pruebas Unitarias (Pytest - 71 tests)
echo   [5] Generar Nuevos Datos Mock Sinteticos (scripts/generate_mock_data.py)
echo   [6] Detener Contenedores Docker y Servidores Locales
echo   [7] Salir
echo.
echo ===============================================================================
set /p "OPT=Seleccione una opcion [1-7] (por defecto 1): "
if "%OPT%"=="" set "OPT=1"

if "%OPT%"=="1" goto :start_all
if "%OPT%"=="2" goto :start_app
if "%OPT%"=="3" goto :start_docker
if "%OPT%"=="4" goto :run_tests
if "%OPT%"=="5" goto :gen_mock
if "%OPT%"=="6" goto :stop_all
if "%OPT%"=="7" exit /b 0

echo [ERROR] Opcion no valida.
pause
goto :show_menu

:: ==============================================================================
:: SUBRUTINAS DE PREPARACION
:: ==============================================================================
:check_env
if not exist ".env" (
    echo [INFO] Archivo .env no encontrado. Creando a partir de .env.example...
    copy .env.example .env >nul
    echo [OK] Archivo .env creado.
)
exit /b 0

:check_venv
call :check_env
if not exist ".venv\Scripts\python.exe" (
    echo [INFO] Entorno virtual .venv no encontrado. Creando .venv...
    python -m venv .venv
    if errorlevel 1 (
        echo [ERROR] No se pudo crear el entorno virtual. Asegurese de tener Python 3.10+ en el PATH.
        pause
        exit /b 1
    )
    echo [INFO] Instalando dependencias desde requirements.txt...
    .\.venv\Scripts\pip.exe install -r requirements.txt
    if errorlevel 1 (
        echo [ERROR] Fallo al instalar dependencias.
        pause
        exit /b 1
    )
    echo [OK] Entorno virtual preparado con exito.
)
exit /b 0

:ensure_docker
set "DOCKER_AVAILABLE=0"

docker version >nul 2>&1
if errorlevel 1 (
    echo [ADVERTENCIA] El comando 'docker' no esta disponible en el PATH de Windows.
    exit /b 0
)

:: 1. Verificar si el demonio de Docker ya esta activo y respondiendo
docker info >nul 2>&1
if not errorlevel 1 (
    echo [OK] El motor de Docker ya se encuentra activo y listo.
    set "DOCKER_AVAILABLE=1"
    exit /b 0
)

:: 2. Si no esta respondiendo, buscar el ejecutable de Docker Desktop
set "DOCKER_EXE="
if exist "%ProgramFiles%\Docker\Docker\Docker Desktop.exe" (
    set "DOCKER_EXE=%ProgramFiles%\Docker\Docker\Docker Desktop.exe"
)
if not defined DOCKER_EXE if exist "%ProgramFiles(x86)%\Docker\Docker\Docker Desktop.exe" (
    set "DOCKER_EXE=%ProgramFiles(x86)%\Docker\Docker\Docker Desktop.exe"
)
if not defined DOCKER_EXE if exist "%LOCALAPPDATA%\Programs\Docker\Docker Desktop.exe" (
    set "DOCKER_EXE=%LOCALAPPDATA%\Programs\Docker\Docker Desktop.exe"
)

:: 3. Iniciar Docker Desktop si fue encontrado
if defined DOCKER_EXE (
    echo [INFO] Docker Desktop no esta activo. Iniciando Docker Desktop automaticamente...
    start "" "!DOCKER_EXE!"
    echo [INFO] Esperando a que el motor de Docker (daemon) termine de inicializar...
    set /a DOCKER_WAIT_COUNT=0
    
    :wait_docker_loop
    timeout /t 3 /nobreak >nul
    docker info >nul 2>&1
    if not errorlevel 1 (
        echo.
        echo [OK] Motor de Docker inicializado y listo para recibir comandos.
        set "DOCKER_AVAILABLE=1"
        exit /b 0
    )
    set /a DOCKER_WAIT_COUNT+=1
    <nul set /p =.
    if !DOCKER_WAIT_COUNT! geq 30 (
        echo.
        echo [ADVERTENCIA] Tiempo de espera agotado esperando a Docker Desktop [90s].
        echo               El stack continuara en modo local [DuckDB + Parquet].
        set "DOCKER_AVAILABLE=0"
        exit /b 0
    )
    goto :wait_docker_loop
) else (
    echo [ADVERTENCIA] No se encontro 'Docker Desktop.exe' en las rutas estandar.
    echo               Abra Docker Desktop manualmente si desea usar contenedores.
    set "DOCKER_AVAILABLE=0"
)
exit /b 0

:: ==============================================================================
:: ACCIONES
:: ==============================================================================

:start_all
cls
echo ===============================================================================
echo                INICIANDO PLATAFORMA MEDALLION COMPLETA
echo ===============================================================================
echo.
call :check_venv
call :ensure_docker

if "!DOCKER_AVAILABLE!"=="1" (
    echo.
    echo [1/3] Levantando stack de infraestructura MinIO, PostgreSQL, Airflow y Spark...
    docker compose -p medallion-platform up -d
    if errorlevel 1 (
        echo [ADVERTENCIA] Hubo un problema al levantar algunos contenedores Docker.
    ) else (
        echo [OK] Contenedores Docker en ejecucion.
    )
) else (
    echo.
    echo [AVISO] Continuando en modo Local con datasets Parquet y DuckDB...
)

echo.
echo [2/3] Levantando Backend FastAPI + Chatbot IA en puerto 8000...
start "Medallion - Backend FastAPI (Puerto 8000)" cmd /k "title Backend FastAPI :8000 && .\.venv\Scripts\python.exe -m uvicorn app.backend.main:app --port 8000 --reload"

echo.
echo [3/3] Levantando Dashboard Streamlit en puerto 8501...
start "Medallion - Dashboard Streamlit (Puerto 8501)" cmd /k "title Dashboard Streamlit :8501 && .\.venv\Scripts\python.exe -m streamlit run dashboards/main.py --server.port 8501"

goto :print_summary

:start_app
cls
echo ===============================================================================
echo          INICIANDO APLICACION WEB (FASTAPI + STREAMLIT EN MODO LOCAL)
echo ===============================================================================
echo.
call :check_venv

echo [1/2] Levantando Backend FastAPI + Chatbot IA en puerto 8000...
start "Medallion - Backend FastAPI (Puerto 8000)" cmd /k "title Backend FastAPI :8000 && .\.venv\Scripts\python.exe -m uvicorn app.backend.main:app --port 8000 --reload"

echo [2/2] Levantando Dashboard Streamlit en puerto 8501...
start "Medallion - Dashboard Streamlit (Puerto 8501)" cmd /k "title Dashboard Streamlit :8501 && .\.venv\Scripts\python.exe -m streamlit run dashboards/main.py --server.port 8501"

goto :print_summary

:start_docker
cls
echo ===============================================================================
echo                     LEVANTANDO STACK DOCKER
echo ===============================================================================
echo.
call :check_env
call :ensure_docker
if "!DOCKER_AVAILABLE!"=="0" (
    echo [ERROR] No se pudo inicializar Docker. Abra Docker Desktop manualmente.
    if "%ACTION%"=="" pause && goto :show_menu
    exit /b 1
)
echo [INFO] Ejecutando docker compose up -d...
docker compose -p medallion-platform up -d
echo [OK] Stack Docker iniciado.
goto :print_summary

:stop_all
cls
echo ===============================================================================
echo                     DETENIENDO SERVICIOS
echo ===============================================================================
echo.
docker info >nul 2>&1
if not errorlevel 1 (
    echo [INFO] Deteniendo contenedores Docker...
    docker compose -p medallion-platform stop
    echo [OK] Contenedores Docker detenidos.
) else (
    echo [INFO] Docker no estaba activo.
)
echo [INFO] Cerrando procesos locales de Python (FastAPI/Streamlit)...
taskkill /FI "WINDOWTITLE eq Medallion - Backend*" /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Medallion - Dashboard*" /T /F >nul 2>&1
echo [OK] Todos los servicios detenidos.
if "%ACTION%"=="" pause && goto :show_menu
exit /b 0

:run_tests
cls
echo ===============================================================================
echo             EJECUTANDO SUITE COMPLETA DE PRUEBAS UNITARIAS (PYTEST)
echo ===============================================================================
echo.
call :check_venv
.\.venv\Scripts\pytest.exe -v
echo.
echo ===============================================================================
if "%ACTION%"=="" pause && goto :show_menu
exit /b 0

:gen_mock
cls
echo ===============================================================================
echo             GENERADOR DE MOCK DATA SINTETICA (JEFRI CALDERIN - US-03)
echo ===============================================================================
echo.
call :check_venv
.\.venv\Scripts\python.exe scripts\generate_mock_data.py --records 5000 --customers 1000 --products 200
echo.
echo [OK] Archivos generados en data/mock/raw y data/mock/gold.
if "%ACTION%"=="" pause && goto :show_menu
exit /b 0

:print_summary
echo.
echo ===============================================================================
echo  [EXITO] SERVICIOS ACTIVOS Y LISTOS PARA USAR
echo ===============================================================================
echo.
echo   * Dashboard Web [Streamlit]  : http://localhost:8501
echo   * Backend API ^& Swagger Docs : http://localhost:8000/docs
echo   * Chatbot Endpoint           : http://localhost:8000/api/v1/chatbot/ask
if "!DOCKER_AVAILABLE!"=="1" (
    echo   * MinIO Storage Console      : http://localhost:9001  [minioadmin / miniopassword123]
    echo   * MinIO S3 API               : http://localhost:9000
    echo   * Airflow Web UI             : http://localhost:8080  [admin / admin]
    echo   * Spark Master Web UI        : http://localhost:8081
)
echo.
echo ===============================================================================
echo  Para detener los servicios:
echo   - Cierra las ventanas emergentes del Backend y Dashboard, o ejecuta: start.bat stop
echo ===============================================================================
echo.
if "%ACTION%"=="" pause
exit /b 0

:show_help
echo ===============================================================================
echo             MEDALLION PLATFORM - AYUDA DE LINEA DE COMANDOS
echo ===============================================================================
echo.
echo   start.bat           Abre el menu interactivo de opciones
echo   start.bat all       Inicia Docker Desktop + Contenedores + FastAPI + Streamlit
echo   start.bat app       Inicia unicamente FastAPI :8000 y Streamlit :8501
echo   start.bat docker    Inicia Docker Desktop y levanta el stack Docker
echo   start.bat stop      Detiene todos los contenedores y servidores locales
echo   start.bat test      Ejecuta la suite de pruebas unitarias con Pytest
echo   start.bat mock      Genera el dataset sintetico de Retail en data/mock/
echo   start.bat help      Muestra esta ayuda
echo.
exit /b 0
