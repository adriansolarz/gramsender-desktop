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
const SUPABASE_SERVICE_ROLE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh3cnhuYnZudHFjeHNhb3hmZGZnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDM1NjUxNSwiZXhwIjoyMDg1OTMyNTE1fQ.HNJ2xkLH49-j8DOp5E4PNy0hbWD4YNOPeTq5_HL3t8g';

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
      // Auto-start backend for returning users
      log(`Auto-starting backend for user: ${savedSession.user.id}`);
      startPythonBackend(savedSession.user.id);
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

// Get path to bundled Python backend executable
function getBundledBackendPath() {
  const exeName = process.platform === 'win32' ? 'gramsender-backend.exe' : 'gramsender-backend';
  
  if (app.isPackaged) {
    // Production: look in resources folder
    return path.join(process.resourcesPath, 'python-dist', exeName);
  } else {
    // Development: look in python-dist folder
    return path.join(__dirname, 'python-dist', exeName);
  }
}

// Start Python backend with user credentials
function startPythonBackend(userId) {
  // #region agent log
  const _dl=require('path'),_df=require('fs'),_dp=_dl.normalize(_dl.join(__dirname,'..','..','..','..','instagram-outreach-system-main (3)','.cursor','debug.log'));try{_df.appendFileSync(_dp,JSON.stringify({location:'main.js:startPythonBackend',message:'startPythonBackend called',data:{userId:userId?userId.substring(0,8):'null',pythonProcessExists:!!pythonProcess,callerStack:new Error().stack.split('\n').slice(1,4).map(s=>s.trim())},timestamp:Date.now()})+'\n');}catch(e){}
  // #endregion
  if (pythonProcess) {
    // #region agent log
    try{_df.appendFileSync(_dp,JSON.stringify({location:'main.js:startPythonBackend:guard',message:'BLOCKED - pythonProcess already exists',data:{pid:pythonProcess.pid},timestamp:Date.now(),hypothesisId:'H2'})+'\n');}catch(e){}
    // #endregion
    console.log('Python backend already running');
    return;
  }

  // #region agent log
  try{_df.appendFileSync(_dp,JSON.stringify({location:'main.js:startPythonBackend:proceed',message:'No existing process, proceeding to start',data:{},timestamp:Date.now(),hypothesisId:'H2'})+'\n');}catch(e){}
  // #endregion

  // Kill any existing process on port 8012 before starting
  try {
    const { execSync } = require('child_process');
    if (process.platform === 'win32') {
      const result = execSync('netstat -ano | findstr :8012 | findstr LISTENING', { encoding: 'utf8', timeout: 5000, windowsHide: true }).trim();
      if (result) {
        const lines = result.split('\n');
        for (const line of lines) {
          const parts = line.trim().split(/\s+/);
          const pid = parts[parts.length - 1];
          if (pid && pid !== '0') {
            log(`Killing zombie process on port 8012: PID ${pid}`);
            // #region agent log
            try{_df.appendFileSync(_dp,JSON.stringify({location:'main.js:killZombie',message:'Killing zombie process on port 8012',data:{pid,line:line.trim()},timestamp:Date.now(),hypothesisId:'H1'})+'\n');}catch(e){}
            // #endregion
            try { execSync(`taskkill /F /PID ${pid}`, { windowsHide: true }); } catch(e) {}
          }
        }
      }
    }
  } catch (e) {
    // No process on port 8012, which is fine
  }

  const backendExe = getBundledBackendPath();
  log(`Looking for backend at: ${backendExe}`);
  
  // Set environment variables for the Python process
  const env = {
    ...process.env,
    STORAGE_MODE: 'supabase',
    SUPABASE_URL: SUPABASE_URL,
    SUPABASE_ANON_KEY: SUPABASE_ANON_KEY,
    SUPABASE_SERVICE_ROLE_KEY: SUPABASE_SERVICE_ROLE_KEY,
    SUPABASE_USER_ID: userId,
    REPLY_MONITOR_ENABLED: 'true',
    ANTI_DETECTION_ENABLED: 'true',
    PYTHONUNBUFFERED: '1'
  };

  try {
    if (fs.existsSync(backendExe)) {
      log(`Starting bundled backend: ${backendExe}`);
      
      pythonProcess = spawn(backendExe, [], {
        env: env,
        stdio: ['pipe', 'pipe', 'pipe']
      });

      pythonProcess.stdout.on('data', (data) => {
        const msg = data.toString();
        console.log(`[Backend] ${msg}`);
        log(`[Backend] ${msg}`);
        if (mainWindow) {
          mainWindow.webContents.send('python-log', msg);
        }
      });

      pythonProcess.stderr.on('data', (data) => {
        const msg = data.toString();
        console.error(`[Backend Error] ${msg}`);
        log(`[Backend Error] ${msg}`);
        if (mainWindow) {
          mainWindow.webContents.send('python-error', msg);
        }
      });

      pythonProcess.on('error', (error) => {
        log(`Backend spawn error: ${error.message}`);
        dialog.showErrorBox(
          'Backend Error',
          `Failed to start backend: ${error.message}`
        );
      });

      pythonProcess.on('close', (code) => {
        log(`Backend process exited with code ${code}`);
        pythonProcess = null;
      });

      log('Backend started successfully');
    } else {
      log(`Backend executable not found at: ${backendExe}`);
      dialog.showErrorBox('Error', `Backend not found.\nExpected: ${backendExe}\n\nPlease reinstall GramSender.`);
    }
  } catch (error) {
    log(`Failed to start backend: ${error.message}`);
    dialog.showErrorBox('Error', `Failed to start backend: ${error.message}`);
  }
}

function stopPythonBackend() {
  // #region agent log
  const _dl2=require('path'),_df2=require('fs'),_dp2=_dl2.normalize(_dl2.join(__dirname,'..','..','..','..','instagram-outreach-system-main (3)','.cursor','debug.log'));try{_df2.appendFileSync(_dp2,JSON.stringify({location:'main.js:stopPythonBackend',message:'stopPythonBackend called',data:{hasProcess:!!pythonProcess,pid:pythonProcess?pythonProcess.pid:null},timestamp:Date.now(),hypothesisId:'H3'})+'\n');}catch(e){}
  // #endregion
  if (pythonProcess) {
    // Kill the process tree on Windows (kills child processes too)
    try {
      if (process.platform === 'win32') {
        const { execSync } = require('child_process');
        execSync(`taskkill /F /T /PID ${pythonProcess.pid}`, { windowsHide: true });
      } else {
        pythonProcess.kill('SIGTERM');
      }
    } catch (e) {
      // Fallback to regular kill
      try { pythonProcess.kill(); } catch(e2) {}
    }
    pythonProcess = null;
    console.log('Python backend stopped');
  }

  // Also clean up port 8012 just in case
  try {
    const { execSync } = require('child_process');
    if (process.platform === 'win32') {
      const result = execSync('netstat -ano | findstr :8012 | findstr LISTENING', { encoding: 'utf8', timeout: 5000, windowsHide: true }).trim();
      if (result) {
        const lines = result.split('\n');
        for (const line of lines) {
          const parts = line.trim().split(/\s+/);
          const pid = parts[parts.length - 1];
          if (pid && pid !== '0') {
            try { execSync(`taskkill /F /PID ${pid}`, { windowsHide: true }); } catch(e) {}
          }
        }
      }
    }
  } catch (e) {}
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
