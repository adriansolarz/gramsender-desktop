@echo off
echo ========================================
echo GramSender Desktop - Build Script
echo ========================================
echo.

echo [1/4] Installing Node.js dependencies...
call npm install
if %ERRORLEVEL% neq 0 (
    echo ERROR: npm install failed
    exit /b 1
)

echo.
echo [2/4] Installing Python dependencies...
cd python-backend
pip install -r requirements.txt --quiet
if %ERRORLEVEL% neq 0 (
    echo ERROR: pip install failed
    exit /b 1
)
cd ..

echo.
echo [3/4] Building Electron app for Windows...
call npm run build:win
if %ERRORLEVEL% neq 0 (
    echo ERROR: Electron build failed
    exit /b 1
)

echo.
echo [4/4] Build complete!
echo.
echo Output: dist\GramSender Setup *.exe
echo.
pause
