import requests
from bs4 import BeautifulSoup
import os

# Base URL of the data
base_url = 'https://coast.noaa.gov/htdata/CMSP/AISDataHandler/'

# Years to download
start_year = 2023
end_year = 2024
years = range(start_year, end_year)

# Directory where the files will be saved
download_dir = os.path.expanduser("./data/")
os.makedirs(download_dir, exist_ok=True)

for year in years:
    year_url = f"{base_url}{year}/"
    response = requests.get(year_url + 'index.html')
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all links to zip files
    links = soup.find_all('a')
    zip_links = [link.get('href') for link in links if link.get('href').endswith('.zip')]

    # Download each zip file
    for link in zip_links:
        file_url = year_url + link
        file_name = os.path.join(download_dir, f"{year}_{link.split('/')[-1]}")
        print(f"Downloading {file_url} to {file_name}")
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            with open(file_name, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Downloaded {file_name}")

print("All files downloaded successfully.")
