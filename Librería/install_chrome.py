import os
import zipfile
import time

# 1. Print link
print("Open https://googlechromelabs.github.io/chrome-for-testing/ to download the files.")

# 2. Set home and target paths
home = os.path.expanduser("~")
download_dir = os.path.join(home, "Downloads")
target_dir = os.path.join(home, "chrome_testing")

# 3. File names and target extraction paths
chromedriver_zip = os.path.join(download_dir, "chromedriver-mac-arm64.zip")
chrome_zip = os.path.join(download_dir, "chrome-mac-arm64.zip")

chromedriver_extract_path = os.path.join(target_dir, "chromedriver-mac-arm64")
chrome_extract_path = os.path.join(target_dir, "chrome-mac-arm64")

chromedriver_path = os.path.join(chromedriver_extract_path, "chromedriver")
chrome_binary_path = os.path.join(
    chrome_extract_path, "Google Chrome for Testing.app", "Contents", "MacOS", "Google Chrome for Testing"
)

# 4. Loop until both zip files are found
while True:
    found_chromedriver = os.path.exists(chromedriver_zip)
    found_chrome = os.path.exists(chrome_zip)

    if found_chromedriver:
        print("✅ chromedriver zip found")
    else:
        print("⏳ chromedriver zip not found. Please download it and press Enter to retry.")
        input()

    if found_chrome:
        print("✅ chrome zip found")
    else:
        print("⏳ chrome zip not found. Please download it and press Enter to retry.")
        input()

    if found_chromedriver and found_chrome:
        print("✅ Both files found.")
        break

# 5. Uncompress each zip into ~/chrome_testing
os.makedirs(target_dir, exist_ok=True)

def unzip_to_target(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(target_dir)
        print(f"✅ Extracted {os.path.basename(zip_path)}")

unzip_to_target(chromedriver_zip, chromedriver_extract_path)
unzip_to_target(chrome_zip, chrome_extract_path)

# 6. Final check
if os.path.exists(chromedriver_path):
    print(f"✅ chromedriver binary exists at:\n{chromedriver_path}")
else:
    print(f"❌ chromedriver binary not found at:\n{chromedriver_path}")

if os.path.exists(chrome_binary_path):
    print(f"✅ Chrome binary exists at:\n{chrome_binary_path}")
else:
    print(f"❌ Chrome binary not found at:\n{chrome_binary_path}")