const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const { spawn } = require('child_process');
const fs = require('fs');

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
  mainWindow = BrowserWindow({
    width: 1200,
    height: 800,
    minWidth: 900,
    minHeight: 600,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.js')
    },
    titleBarStyle: 'default',
    show: false,
    backgroundColor: '#0f172a'
  });

  // Check if user is already logged in
  const savedSession = loadAuthToken();
  if (savedSession && savedSession.access_token) {
    // Verify token is still valid
    currentUser = savedSession.user;
    mainWindow.loadFile('pages/dashboard.html');
  } else {
    mainWindow.loadFile('pages/login.html');
  }

  mainWindow.once('ready-to-show', () => {
    mainWindow.show();
  });

  mainWindow.on('closed', () => {
    mainWindow = null;
    stopPythonBackend();
  });
}

// Start Python backend with user credentials
function startPythonBackend(userId) {
  if (pythonProcess) {
    console.log('Python backend already running');
    return;
  }

  const pythonPath = getResourcePath('python-backend');
  const pythonExe = process.platform === 'win32' ? 'python.exe' : 'python3';
  
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
      pythonProcess = spawn(pythonExe, [mainPy], {
        cwd: pythonPath,
        env: env,
        stdio: ['pipe', 'pipe', 'pipe']
      });

      pythonProcess.stdout.on('data', (data) => {
        console.log(`[Python] ${data}`);
        if (mainWindow) {
          mainWindow.webContents.send('python-log', data.toString());
        }
      });

      pythonProcess.stderr.on('data', (data) => {
        console.error(`[Python Error] ${data}`);
        if (mainWindow) {
          mainWindow.webContents.send('python-error', data.toString());
        }
      });

      pythonProcess.on('close', (code) => {
        console.log(`Python process exited with code ${code}`);
        pythonProcess = null;
      });

      console.log('Python backend started');
    } else {
      console.error('Python backend not found at:', mainPy);
    }
  } catch (error) {
    console.error('Failed to start Python backend:', error);
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
app.whenReady().then(createWindow);

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
