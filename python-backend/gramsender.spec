# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec file for GramSender Python backend.
Creates a standalone executable with all dependencies bundled.
"""

import sys
from PyInstaller.utils.hooks import collect_all, collect_submodules

block_cipher = None

# Collect all instagrapi dependencies
instagrapi_datas, instagrapi_binaries, instagrapi_hiddenimports = collect_all('instagrapi')
supabase_datas, supabase_binaries, supabase_hiddenimports = collect_all('supabase')

# Hidden imports that PyInstaller might miss
hidden_imports = [
    'uvicorn',
    'uvicorn.logging',
    'uvicorn.loops',
    'uvicorn.loops.auto',
    'uvicorn.protocols',
    'uvicorn.protocols.http',
    'uvicorn.protocols.http.auto',
    'uvicorn.protocols.websockets',
    'uvicorn.protocols.websockets.auto',
    'uvicorn.lifespan',
    'uvicorn.lifespan.on',
    'fastapi',
    'starlette',
    'pydantic',
    'pydantic_core',
    'cryptography',
    'websockets',
    'httpx',
    'httpcore',
    'anyio',
    'sniffio',
    'h11',
    'PIL',
    'PIL.Image',
    'requests',
    'urllib3',
    'certifi',
    'charset_normalizer',
    'idna',
    'json',
    'email',
    'email.mime',
    'email.mime.text',
    'email.mime.multipart',
    'multipart',
    'python_multipart',
    'gotrue',
    'postgrest',
    'realtime',
    'storage3',
    'supafunc',
]
hidden_imports.extend(instagrapi_hiddenimports)
hidden_imports.extend(supabase_hiddenimports)
hidden_imports.extend(collect_submodules('instagrapi'))
hidden_imports.extend(collect_submodules('supabase'))

a = Analysis(
    ['run.py'],
    pathex=[],
    binaries=instagrapi_binaries + supabase_binaries,
    datas=instagrapi_datas + supabase_datas,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='gramsender-backend',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,  # Keep console for logging
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
