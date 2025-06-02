"""
helper.py

Author: Sunny Hospital (PolarWatch)
Date: 2024-05-29

Utility functions for downloading and handling remote files from the NOAA PolarWatch ERDDAP server.
Includes functions for scraping file links, downloading files, and unzipping gzipped NetCDF files.

Functions:
    download_files_from_server(server: str, year: str, dir_path: str) -> None
        Downloads all .nc, .zip, and .gz files for a given year from the specified server directory
        and saves them to a local directory.

    download_file(url: str, path: str) -> None
        Downloads a single file from a URL and saves it to the specified local path.

    unzip_to_nc(fpath: str) -> BytesIO
        Unzips a .gz file and returns its content as a BytesIO object.
"""
import requests
import os
import gzip
from bs4 import BeautifulSoup
from io import BytesIO
import xarray as xr


def download_files_from_server(server: str, year: str,  dir_path: str)-> None:
    """Downloads files from a NOAA ERDDAP server for a specific year.

    Args:
        server (str): Base URL of the server.
        year (str): Four-digit year (subdirectory of files).
        dir_path (str): Full local data root directory.

    The function creates a local directory for the year if it does not exist,
    scrapes the server directory for .nc, .zip, and .gz files, and downloads them.
    """
    server = "https://polarwatch.noaa.gov/erddap/files/usnic_ims_4km"

    if not os.path.exists(f'{dir_path}/{year}'):
        print(f'{dir_path}/{year} doesn\'t exist. Creating directory ..')    
        os.makedirs(f'{dir_path}/{year}')

    url = f"{server}/{year}/"
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')

    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.endswith(('.nc', '.zip', '.gz')):
            file_url = url + href
            file_path = os.path.join(f'{dir_path}/{year}', href)
            download_file(file_url, file_path)


def download_file(url, path):
    """Downloads a file from the input URL and saves it to the specified path.

    Args:
        url (str): URL of the file to download.
        path (str): Full local file path where the file will be saved.
    """
    response = requests.get(url, stream=True)
    response.raise_for_status()   

    with open(path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    print(f"Downloaded: {path}")

def unzip_to_nc(fpath) -> BytesIO:
    """Unzips a .gz file and returns its content as a BytesIO object.

    Args:
        fpath (str): Full file path of the .gz file.

    Returns:
        BytesIO: BytesIO object containing the unzipped file content.
    """
    # Open the gzip file
    with gzip.open(fpath, 'rb') as gz:
        # Read all content from gzip into memory
        gz_content = gz.read()
    nc_file = BytesIO(gz_content)
    return nc_file

def load_nc_file(fpath) -> xr.Dataset:
    """loads data from a gzipped or nc local file.

    Args:
        fpath (str): full path of a file to be loaded

    Raises:
        ValueError: If the file extension is not nc.gz or .nc

    Returns:
        None: Modifies self.ds directly.

    Usage:
        load_nc_file(filepath)
    """
    if not (fpath.endswith('.gz') or fpath.endswith('.nc')):
        raise ValueError("File extension must be .gz or .nc")

    if fpath.endswith('.gz'):
        nc_file = unzip_to_nc(fpath)
    else:
        nc_file = fpath

    ds = xr.open_dataset(nc_file, engine='h5netcdf')
    return ds

def load_mfiles(flist):
        """loads data from multiple files

        Args:
            flist (list): list of full filename paths

        Raises:
            ValueError: If the file extension is not nc.gz or .nc 

        Returns:
            None: Modifies self.ds directly.           
        Usage:
            load_mfiles(flist)
        """
        try:
            tot_ds = []
            for f in flist:
  
                try:
                    #print(f"Processing file: {f}")
                    if not (f.endswith('.gz') or f.endswith('.nc')):
                        raise ValueError("File extension must be .gz or .nc")

                    if f.endswith('.gz'):
                        nc_file = unzip_to_nc(f)
                    else:
                        nc_file = f

                    da = xr.open_dataset(nc_file)#, engine='h5netcdf')
                    da.load()
                    # Append to list
                    tot_ds.append(da)
                except Exception as e:
                    print(f"Failed to load file: {e}")
            
            if not tot_ds:
                raise ValueError("no file was loaded. Check the file path")
            
            ds = xr.concat(tot_ds, dim='time')
            print("Successfully loaded list of files")
            return ds
        
        except Exception as e:
            print(f"Failed to get files:  {e}")
            raise

def load_mfiles(flist):
        """loads data from multiple files

        Args:
            flist (list): list of full filename paths

        Raises:
            ValueError: If the file extension is not nc.gz or .nc 

        Returns:
            None: Modifies self.ds directly.           
        Usage:
            load_mfiles(flist)
        """
        try:
            tot_ds = []
            for f in flist:
  
                try:
                    #print(f"Processing file: {f}")
                    if not (f.endswith('.gz') or f.endswith('.nc')):
                        raise ValueError("File extension must be .gz or .nc")

                    if f.endswith('.gz'):
                        nc_file = unzip_to_nc(f)
                    else:
                        nc_file = f

                    da = xr.open_dataset(nc_file)#, engine='h5netcdf')
                    da.load()
                    # Append to list
                    tot_ds.append(da)
                except Exception as e:
                    print(f"Failed to load file: {e}")
            
            if not tot_ds:
                raise ValueError("no file was loaded. Check the file path")
            
            ds = xr.concat(tot_ds, dim='time')
            print("Successfully loaded list of files")
            return ds
        
        except Exception as e:
            print(f"Failed to get files:  {e}")
            raise
