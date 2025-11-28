import os
import re
import requests, sys
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm.auto import tqdm

def get_valid_filename(response):
    """
    Determines the correct filename from Content-Disposition header or URL.
    """
    filename = None
    # Try to get filename from Content-Disposition header
    cd = response.headers.get("content-disposition")
    if cd:
        # Look for filename="name" or filename=name
        fname_match = re.findall(r'filename\*?=([^;]+)', cd, flags=re.IGNORECASE)
        if fname_match:
            # Clean up the found name (remove " and ' and UTF-8 markers)
            clean_name = fname_match[0].strip().strip('"').strip("'")
            if "UTF-8''" in clean_name:
                clean_name = clean_name.split("UTF-8''")[-1]
            filename = unquote(clean_name)

    if not filename or filename.strip() == "":
        return None

    # Sanitize filename (remove illegal chars for OS)
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def download_chunk(url, start, end, file_path, bar, session):
    """
    Worker function to download a specific byte range.
    """
    retries = 3
    while retries > 0:
        retries -= 1
        headers = {'Range': f'bytes={start}-{end}'}
        try:
            with session.get(url, headers=headers, stream=True, timeout=10) as r:
                r.raise_for_status()
                with open(file_path, 'r+b') as f:
                    f.seek(start)
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            start += len(chunk)
                            bar.update(len(chunk))
            return True
        except IOError as e:
            raise e
        except Exception as e:
            if retries == 0:
                raise e
    return False
    
def get_full_path(out_path, filename):
    _,ext = os.path.splitext(out_path)
    if os.path.isdir(out_path) or len(ext) < 2:
        final_path = os.path.join(out_path, filename)
        os.makedirs(out_path, exist_ok=True)
    else:
        # Assume out_path is the full desired path
        final_path = out_path
        filename = os.path.basename(final_path)
        # Create parent directories if they don't exist
        os.makedirs(os.path.dirname(os.path.abspath(final_path)), exist_ok=True)
    return final_path, filename

def download_file(url, out_path=None, max_concurrent_connections=12):
    """
    Downloads a file from a URL with multi-connection support and visual progress.

    Args:
        url (str): The target URL.
        out_path (str | None): Directory or full file path. 
                               If None, returns filename info without downloading.
        max_concurrent_connections (int): Number of threads for downloading.

    Returns:
        str: The final output file path on success.
        None: On failure.
    """
    filename = os.path.basename(url)
    if '.' in filename:
        if out_path is None:
            return filename
        final_path, filename = get_full_path(out_path, filename)
        if os.path.isfile(final_path):
#            print(f"File already exists: {final_path}")
            return filename
    else:
        filename = None

    session = requests.Session()
    
    try:
        # 1. Initial Head Request to get headers and resolve redirects
        # We use stream=True to avoid downloading body, but ensure we follow redirects
        with session.get(url, stream=True, allow_redirects=True) as initial_response:
            final_url = initial_response.url
            file_size = int(initial_response.headers.get('content-length', 0))
            accept_ranges = initial_response.headers.get('accept-ranges', 'none')
            
            # Determine filename
            if not filename:
                filename = get_valid_filename(initial_response)
                if not filename or out_path is None:
                    return filename
                final_path, filename = get_full_path(out_path, filename)
                if os.path.isfile(final_path):
#                    print(f"File already exists: {final_path}")
                    return filename
                
        if not filename:
            return filename
        
        print(f"Downloading: {filename}")
#        print(f"Size: {file_size / (1024*1024):.2f} MB")
#        print(f"Saving to: {final_path}")

        # 4. Check for Multi-part Support
        # If server doesn't support ranges or size is unknown, fallback to single stream
        if file_size == 0 or accept_ranges == 'none':
            print("Server does not support resume/ranges. Switching to single connection.")
            max_concurrent_connections = 1
        else:
            max_concurrent_connections = min(max_concurrent_connections, int(file_size / 5E+08) + 1)

        # 5. Prepare File on Disk
        # Create empty file of specific size to allow random access writes
        with open(final_path, 'wb') as f:
            if file_size > 0:
                f.truncate(file_size)

        # 6. Calculate Ranges
        chunk_size = file_size // max_concurrent_connections
        ranges = []
        for i in range(max_concurrent_connections):
            start = i * chunk_size
            if i == max_concurrent_connections - 1:
                end = file_size - 1  # Last chunk takes the remainder
            else:
                end = (i + 1) * chunk_size - 1
            ranges.append((start, end))

        # 7. Start Download
        # unit_scale=True makes 1024 -> 1k, etc.
        with tqdm(total=file_size, unit='B', unit_scale=True, file=sys.stdout) as bar:
            if max_concurrent_connections > 1:
                with ThreadPoolExecutor(max_workers=max_concurrent_connections) as executor:
                    futures = []
                    for start, end in ranges:
                        futures.append(
                            executor.submit(download_chunk, final_url, start, end, final_path, bar, session)
                        )
                    
                    # Wait for all chunks
                    for future in as_completed(futures):
                        if not future.result():
                            print("\nError occurred in one of the download threads.")
                            return None
            else:
                # Single connection fallback
                with session.get(final_url, stream=True) as r:
                    r.raise_for_status()
                    with open(final_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                bar.update(len(chunk))

        return filename

    except KeyboardInterrupt:
        print("\nDownload cancelled.")
        if out_path and os.path.exists(final_path):
            os.remove(final_path) # Clean up partial file
        return None
    except Exception as e:
        print(f"\nDownload failed: {e}")
        if out_path and os.path.exists(final_path):
            os.remove(final_path) # Clean up partial file
        return None
    finally:
        session.close()