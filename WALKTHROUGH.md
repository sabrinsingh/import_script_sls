# Bundle Python App - Walkthrough

## Completed Tasks
- [x] **Refactoring**: Modified `main_script.py` and `gui_runner.py` to allow direct execution without `subprocess`.
- [x] **Environment Setup**: Installed `PyInstaller`.
- [x] **Building (Local)**: Created an Apple Silicon version locally.
- [x] **Building (Universal)**: configured a GitHub Actions workflow to build for both Intel and Apple Silicon automatically.

## How to Build for Everyone (Intel + Apple Silicon)
Since your friends have different internal chips (Intel vs M1/M2/M3), and we cannot build for Intel on your M-series Mac easily, we use GitHub Actions.

1.  **Push to GitHub**:
    ```bash
    git add .
    git commit -m "Add build workflow"
    # Create a new repo on GitHub and follow instructions to push, e.g.:
    # git remote add origin <your-repo-url>
    # git push -u origin main
    ```
2.  **Wait for Build**:
    -   Go to your GitHub repository -> **Actions** tab.
    -   You will see a "Build Mac Apps" workflow running.
    -   Wait for it to turn green (Success).
3.  **Download Apps**:
    -   Click on the completed workflow run.
    -   Scroll down to **Artifacts**.
    -   Download `Stage1,3_Import_GUI_macos-13` (Intel) and `Stage1,3_Import_GUI_macos-14` (Apple Silicon).
4.  **Distribute**:
    -   Send the Intel version to Intel friends.
    -   Send the Apple Silicon version to M1/M2 friends.

## Local Artifacts (For you only)
- `dist/Stage1,3_Import_GUI.app` (Runs on your Mac only).
