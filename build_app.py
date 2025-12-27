import PyInstaller.__main__
import shutil
import os

print("🚀 Starting PyInstaller Build...")

# Clean previous builds
if os.path.exists("build"):
    shutil.rmtree("build")
if os.path.exists("dist"):
    shutil.rmtree("dist")

PyInstaller.__main__.run([
    'gui_runner.py',
    '--name=Stage1,3_Import_GUI',
    '--onefile',
    '--windowed',
    '--collect-all=ttkbootstrap',
    '--hidden-import=psycopg2',
    '--hidden-import=PIL',
    '--hidden-import=boto3',
    '--exclude-module=matplotlib',
    '--exclude-module=scipy',
    '--exclude-module=sklearn',
    '--exclude-module=statsmodels',
    '--exclude-module=sphinx',
    '--exclude-module=notebook',
    '--exclude-module=jupyter',
    '--exclude-module=torch',
    '--exclude-module=tensorflow',
    '--exclude-module=nltk',
    '--clean',
    '--noconfirm',
])

print("✅ Build Complete! Check the 'dist' folder.")
