const { contextBridge, ipcRenderer } = require('electron');

// Expose protected methods to the renderer process
contextBridge.exposeInMainWorld('electronAPI', {
  // Config
  getConfig: () => ipcRenderer.invoke('get-config'),
  
  // Auth
  loginSuccess: (session) => ipcRenderer.invoke('login-success', session),
  logout: () => ipcRenderer.invoke('logout'),
  getCurrentUser: () => ipcRenderer.invoke('get-current-user'),
  getSavedSession: () => ipcRenderer.invoke('get-saved-session'),
  
  // Backend control
  startBackend: (userId) => ipcRenderer.invoke('start-backend', userId),
  stopBackend: () => ipcRenderer.invoke('stop-backend'),
  getBackendStatus: () => ipcRenderer.invoke('get-backend-status'),
  
  // Event listeners
  onPythonLog: (callback) => ipcRenderer.on('python-log', (event, data) => callback(data)),
  onPythonError: (callback) => ipcRenderer.on('python-error', (event, data) => callback(data))
});
