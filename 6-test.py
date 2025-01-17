import requests
from bs4 import BeautifulSoup
import os

# Base URL of the data
base_url = 'https://coast.noaa.gov/htdata/CMSP/AISDataHandler/'

# Specific year and months to download
year = 2023
months = ["01", "02"]

# Directory where the files will be saved
download_dir = os.path.expanduser("./data/")
os.makedirs(download_dir, exist_ok=True)

# Construct the URL for the specific year
year_url = f"{base_url}{year}/"
response = requests.get(year_url + 'index.html')
soup = BeautifulSoup(response.text, 'html.parser')

# Find all links to zip files
links = soup.find_all('a')
zip_links = [link.get('href') for link in links if link.get('href').endswith('.zip')]

# Filter links to include only files for January and February
filtered_links = [link for link in zip_links if any(f"_{year}_{month}_" in link for month in months)]

# Download each filtered zip file
for link in filtered_links:
    file_url = year_url + link
    file_name = os.path.join(download_dir, f"{year}_{link.split('/')[-1]}")
    print(f"Downloading {file_url} to {file_name}")
    with requests.get(file_url, stream=True) as r:
        r.raise_for_status()
        with open(file_name, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"Downloaded {file_name}")

print("All January and February 2023 files downloaded successfully.")