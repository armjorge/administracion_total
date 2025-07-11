
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import platform
import os

def load_chrome(directory):
    """Launch Chrome with OS-specific paths and consistent configuration."""

    # Detect OS
    system = platform.system()
    home = os.path.expanduser("~")
    # Set Chrome binary and ChromeDriver paths based on OS
    if system == "Windows":
        chrome_binary_path = os.path.join(home, "Documents", "chrome-win64", "chrome.exe")
        chromedriver_path = os.path.join(home, "Documents", "chromedriver-win64", "chromedriver.exe")
    elif system == "Darwin":  # macOS
        
        chrome_binary_path = os.path.join(home, "chrome_testing", "chrome-mac-arm64", "Google Chrome for Testing.app", "Contents", "MacOS", "Google Chrome for Testing")
        chromedriver_path = os.path.join(home, "chrome_testing", "chromedriver-mac-arm64", "chromedriver")
    else:
        print(f"Unsupported OS: {system}")
        return None

    # Set Chrome options
    chrome_options = Options()
    chrome_options.binary_location = chrome_binary_path

    prefs = {
        "download.default_directory": os.path.abspath(directory),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    # (Optional) Further reduce noise:
    chrome_options.add_argument("--disable-background-networking")
    chrome_options.add_argument("--disable-client-side-phishing-detection")
    chrome_options.add_argument("--disable-component-update")

    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")

    try:
        # Initialize ChromeDriver with the correct service path
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
    except Exception as e:
        print(f"Failed to initialize Chrome driver: {e}")
        return None