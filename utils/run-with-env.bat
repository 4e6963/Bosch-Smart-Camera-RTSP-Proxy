@echo off
setlocal enabledelayedexpansion

rem Loads ..\.env (relative to this script) into the environment if present, then
rem runs `cargo run -- %*` from the repo root. .env is optional -- every setting
rem has a built-in default -- so a missing file only warns, it doesn't stop the
rem run. Usage: utils\run-with-env.bat login

set "REPO_ROOT=%~dp0.."
set "ENV_FILE=%REPO_ROOT%\.env"

if not exist "%ENV_FILE%" (
    echo WARNING: %ENV_FILE% not found; continuing with built-in defaults ^(copy .env.example to .env to override^).
) else (
    for /f "usebackq tokens=1,* delims==" %%i in (`findstr /v "^#" "%ENV_FILE%"`) do (
        if not "%%i"=="" set "%%i=%%j"
    )
)

pushd "%REPO_ROOT%"
cargo run -- %*
set "EXIT_CODE=%ERRORLEVEL%"
popd

exit /b %EXIT_CODE%
