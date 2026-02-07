const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const { spawn } = require('child_process');
const fs = require('fs');

// Enable logging to file for debugging
const logFile = path.join(app.getPath('userData'), 'gramsender.log');
function log(message) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}\n`;
  console.log(message);
  try {
    fs.appendFileSync(logFile, logMessage);
  } catch (e) {
    // Ignore log errors
  }
}

// Catch uncaught exceptions
process.on('uncaughtException', (error) => {
  log(`Uncaught Exception: ${error.message}\n${error.stack}`);
  dialog.showErrorBox('Error', `An error occurred: ${error.message}`);
});

log('App starting...');

// Supabase configuration - hardcoded for security
const SUPABASE_URL = 'https://hwrxnbvntqcxsaoxfdfg.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh3cnhuYnZudHFjeHNhb3hmZGZnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAzNTY1MTUsImV4cCI6MjA4NTkzMjUxNX0.7uLU_K8yahphqLkVtJFzQHypK7Q5ySxx4I1U-p1zYB8';

let mainWindow;
let pythonProcess = null;
let currentUser = null;

// Get the path to resources (works in dev and production)
function getResourcePath(relativePath) {
  if (app.isPackaged) {
    return path.join(process.resourcesPath, relativePath);
  }
  return path.join(__dirname, relativePath);
}

// Get user data directory for storing auth tokens
function getUserDataPath() {
  return app.getPath('userData');
}

// Save auth token
function saveAuthToken(session) {
  const tokenPath = path.join(getUserDataPath(), 'auth.json');
  fs.writeFileSync(tokenPath, JSON.stringify(session, null, 2));
}

// Load auth token
function loadAuthToken() {
  const tokenPath = path.join(getUserDataPath(), 'auth.json');
  if (fs.existsSync(tokenPath)) {
    try {
      return JSON.parse(fs.readFileSync(tokenPath, 'utf8'));
    } catch (e) {
      return null;
    }
  }
  return null;
}

// Clear auth token
function clearAuthToken() {
  const tokenPath = path.join(getUserDataPath(), 'auth.json');
  if (fs.existsSync(tokenPath)) {
    fs.unlinkSync(tokenPath);
  }
}

function createWindow() {
  log('Creating window...');
  
  try {
    const preloadPath = path.join(__dirname, 'preload.js');
    log(`Preload path: ${preloadPath}`);
    log(`Preload exists: ${fs.existsSync(preloadPath)}`);
    
    mainWindow = new BrowserWindow({
      width: 1200,
      height: 800,
      minWidth: 900,
      minHeight: 600,
      webPreferences: {
        nodeIntegration: false,
        contextIsolation: true,
        preload: preloadPath
      },
      titleBarStyle: 'default',
      show: true,
      backgroundColor: '#0f172a'
    });

    log('BrowserWindow created');

    // Check if user is already logged in
    const savedSession = loadAuthToken();
    log(`Saved session exists: ${!!savedSession}`);
    
    let pageToLoad;
    if (savedSession && savedSession.access_token) {
      currentUser = savedSession.user;
      pageToLoad = path.join(__dirname, 'pages', 'dashboard.html');
    } else {
      pageToLoad = path.join(__dirname, 'pages', 'login.html');
    }
    
    log(`Loading page: ${pageToLoad}`);
    log(`Page exists: ${fs.existsSync(pageToLoad)}`);
    
    mainWindow.loadFile(pageToLoad).then(() => {
      log('Page loaded successfully');
    }).catch((err) => {
      log(`Failed to load page: ${err.message}`);
      dialog.showErrorBox('Error', `Failed to load page: ${err.message}`);
    });

    mainWindow.once('ready-to-show', () => {
      log('Window ready to show');
      mainWindow.show();
      mainWindow.focus();
    });
    
    mainWindow.webContents.on('did-fail-load', (event, errorCode, errorDescription) => {
      log(`Page failed to load: ${errorCode} - ${errorDescription}`);
    });
    
  } catch (error) {
    log(`Failed to create window: ${error.message}\n${error.stack}`);
    dialog.showErrorBox('Error', `Failed to create window: ${error.message}`);
  }

  if (mainWindow) {
    mainWindow.on('closed', () => {
      log('Window closed');
      mainWindow = null;
      stopPythonBackend();
    });
  }
}

// Find Python executable
function findPython() {
  const { execSync } = require('child_process');
  
  // List of possible Python commands to try
  const pythonCommands = process.platform === 'win32' 
    ? ['python', 'python3', 'py', 'py -3']
    : ['python3', 'python'];
  
  // Common Python installation paths on Windows
  const windowsPaths = [
    path.join(process.env.LOCALAPPDATA || '', 'Programs', 'Python', 'Python311', 'python.exe'),
    path.join(process.env.LOCALAPPDATA || '', 'Programs', 'Python', 'Python310', 'python.exe'),
    path.join(process.env.LOCALAPPDATA || '', 'Programs', 'Python', 'Python39', 'python.exe'),
    path.join(process.env.LOCALAPPDATA || '', 'Programs', 'Python', 'Python312', 'python.exe'),
    path.join(process.env.LOCALAPPDATA || '', 'Programs', 'Python', 'Python313', 'python.exe'),
    'C:\\Python311\\python.exe',
    'C:\\Python310\\python.exe',
    'C:\\Python39\\python.exe',
    'C:\\Python312\\python.exe',
  ];
  
  // Try commands in PATH first
  for (const cmd of pythonCommands) {
    try {
      const result = execSync(`${cmd} --version`, { 
        encoding: 'utf8', 
        timeout: 5000,
        windowsHide: true,
        stdio: ['pipe', 'pipe', 'pipe']
      });
      log(`Found Python via command '${cmd}': ${result.trim()}`);
      return cmd;
    } catch (e) {
      // Command not found, try next
    }
  }
  
  // Try absolute paths on Windows
  if (process.platform === 'win32') {
    for (const pythonPath of windowsPaths) {
      if (fs.existsSync(pythonPath)) {
        log(`Found Python at: ${pythonPath}`);
        return pythonPath;
      }
    }
  }
  
  return null;
}

// Start Python backend with user credentials
function startPythonBackend(userId) {
  if (pythonProcess) {
    console.log('Python backend already running');
    return;
  }

  const pythonPath = getResourcePath('python-backend');
  
  // Find Python executable
  const pythonExe = findPython();
  
  if (!pythonExe) {
    log('Python not found on system');
    dialog.showErrorBox(
      'Python Required',
      'GramSender requires Python 3.9+ to run.\n\n' +
      'Please install Python from:\nhttps://www.python.org/downloads/\n\n' +
      'Make sure to check "Add Python to PATH" during installation.\n\n' +
      'After installing, restart GramSender.'
    );
    return;
  }
  
  log(`Using Python: ${pythonExe}`);
  
  // Set environment variables for the Python process
  const env = {
    ...process.env,
    STORAGE_MODE: 'supabase',
    SUPABASE_URL: SUPABASE_URL,
    SUPABASE_ANON_KEY: SUPABASE_ANON_KEY,
    SUPABASE_USER_ID: userId,
    REPLY_MONITOR_ENABLED: 'true',
    ANTI_DETECTION_ENABLED: 'true'
  };

  try {
    // Try to run the bundled Python or system Python
    const mainPy = path.join(pythonPath, 'run.py');
    
    if (fs.existsSync(mainPy)) {
      log(`Starting Python backend: ${pythonExe} ${mainPy}`);
      
      // Handle 'py -3' command which needs shell
      const spawnOptions = {
        cwd: pythonPath,
        env: env,
        stdio: ['pipe', 'pipe', 'pipe'],
        shell: pythonExe.includes(' ')  // Use shell if command has spaces
      };
      
      pythonProcess = spawn(pythonExe, [mainPy], spawnOptions);

      pythonProcess.stdout.on('data', (data) => {
        const msg = data.toString();
        console.log(`[Python] ${msg}`);
        log(`[Python] ${msg}`);
        if (mainWindow) {
          mainWindow.webContents.send('python-log', msg);
        }
      });

      pythonProcess.stderr.on('data', (data) => {
        const msg = data.toString();
        console.error(`[Python Error] ${msg}`);
        log(`[Python Error] ${msg}`);
        if (mainWindow) {
          mainWindow.webContents.send('python-error', msg);
        }
      });

      pythonProcess.on('error', (error) => {
        log(`Python spawn error: ${error.message}`);
        dialog.showErrorBox(
          'Python Error',
          `Failed to start Python backend: ${error.message}\n\n` +
          'Please ensure Python 3.9+ is installed and in your PATH.'
        );
      });

      pythonProcess.on('close', (code) => {
        log(`Python process exited with code ${code}`);
        pythonProcess = null;
      });

      log('Python backend started');
    } else {
      log(`Python backend not found at: ${mainPy}`);
      dialog.showErrorBox('Error', `Python backend files not found.\nExpected: ${mainPy}`);
    }
  } catch (error) {
    log(`Failed to start Python backend: ${error.message}`);
    dialog.showErrorBox('Error', `Failed to start Python backend: ${error.message}`);
  }
}

function stopPythonBackend() {
  if (pythonProcess) {
    pythonProcess.kill();
    pythonProcess = null;
    console.log('Python backend stopped');
  }
}

// IPC Handlers
ipcMain.handle('get-config', () => {
  return {
    supabaseUrl: SUPABASE_URL,
    supabaseAnonKey: SUPABASE_ANON_KEY
  };
});

ipcMain.handle('login-success', (event, session) => {
  currentUser = session.user;
  saveAuthToken(session);
  startPythonBackend(session.user.id);
  mainWindow.loadFile('pages/dashboard.html');
});

ipcMain.handle('logout', () => {
  currentUser = null;
  clearAuthToken();
  stopPythonBackend();
  mainWindow.loadFile('pages/login.html');
});

ipcMain.handle('get-current-user', () => {
  return currentUser;
});

ipcMain.handle('get-saved-session', () => {
  return loadAuthToken();
});

ipcMain.handle('start-backend', (event, userId) => {
  startPythonBackend(userId);
});

ipcMain.handle('stop-backend', () => {
  stopPythonBackend();
});

ipcMain.handle('get-backend-status', () => {
  return pythonProcess !== null;
});

// App lifecycle
app.whenReady().then(() => {
  log('App is ready');
  createWindow();
}).catch((err) => {
  log(`App ready failed: ${err.message}`);
});

app.on('window-all-closed', () => {
  stopPythonBackend();
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow();
  }
});

app.on('before-quit', () => {
  stopPythonBackend();
});
