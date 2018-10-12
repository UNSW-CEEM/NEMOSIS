# -*- mode: python -*-

block_cipher = None


a = Analysis(['gui.py'],
             pathex=['C:\\Users\\user\\Documents\\GitHub\\nem-data'],
             binaries=[],
             datas=[],
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)


a.datas += [('favicon.ico', 'C:\\Users\\user\\Documents\\GitHub\\nem-data\\favicon.ico',  'DATA')]

pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='gui',
          debug=False,
          strip=False,
          upx=True,
          runtime_tmpdir=None,
          console=True , icon='favicon.ico')
