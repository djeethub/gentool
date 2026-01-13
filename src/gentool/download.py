import os
import re
import requests, sys
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm.auto import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class SpaceMap:
    def __init__(self, total_space: int):
        if total_space <= 0:
            raise ValueError("Total space must be greater than 0.")
        
        self.total_space = total_space
        self.occupied_intervals = []

    def get_next_available(self, length: int):
        if length <= 0:
            raise ValueError("Length must be greater than 0.")

        current_ptr = 0
        for start, end in self.occupied_intervals:
            gap_size = start - current_ptr
            if gap_size > 0:
                return (current_ptr, min(current_ptr + length - 1, start - 1))
            current_ptr = max(current_ptr, end + 1)

        if (self.total_space - current_ptr) > 0:
            return (current_ptr, min(current_ptr + length - 1, self.total_space - 1))

        return None

    def fill(self, start: int, end: int):
        if start > end:
            raise ValueError("Start position cannot be greater than end position.")
        if start < 0 or end >= self.total_space:
            raise ValueError(f"Range {start}-{end} is out of bounds (0-{self.total_space-1}).")

        self.occupied_intervals.append((start, end))
        self.occupied_intervals.sort(key=lambda x: x[0])
        merged = []
        if not self.occupied_intervals:
            return

        current_start, current_end = self.occupied_intervals[0]
        for i in range(1, len(self.occupied_intervals)):
            next_start, next_end = self.occupied_intervals[i]

            if next_start <= current_end + 1:
                current_end = max(current_end, next_end)
            else:
                merged.append((current_start, current_end))
                current_start, current_end = next_start, next_end

        merged.append((current_start, current_end))
        
        self.occupied_intervals = merged

    def vacant(self, start: int, end: int):
        if start > end:
            raise ValueError("Start position cannot be greater than end position.")
        
        new_intervals = []
        for occ_start, occ_end in self.occupied_intervals:
            if occ_end < start or occ_start > end:
                new_intervals.append((occ_start, occ_end))
            else:
                if occ_start < start:
                    new_intervals.append((occ_start, start - 1))
                if occ_end > end:
                    new_intervals.append((end + 1, occ_end))
        
        self.occupied_intervals = new_intervals

    def reset(self):
        self.occupied_intervals = []

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

def download_chunk(url, param, file_path, bar, session):
    """
    Worker function to download a specific byte range.
    """
    headers = {'Range': f'bytes={param[0]}-{param[1]}'}
    try:
        with session.get(url, headers=headers, stream=True, timeout=15) as r:
            r.raise_for_status()
            with open(file_path, 'r+b') as f:
                f.seek(param[0])
                for chunk in r.iter_content(chunk_size=128*1024):
                    if chunk:
                        f.write(chunk)
                        l = len(chunk)
                        if param[0] + l > param[1]:
                            bar.update(param[1] - param[0] + 1)
                            break
                        else:
                            param[0] += l
                            bar.update(l)
        return True, param[0], param[1], None
    except Exception as e:
        return False, param[0], param[1], e
    
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

def download_file(url, out_path=None, max_concurrent_connections=8, min_chunk_size=1024*1024):
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

    # Configure the HTTPAdapter
    adapter = HTTPAdapter(
        pool_connections=10, # Number of connection pools
        pool_maxsize=20, # Max connections per pool
        max_retries=Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504]) # Retry settings
    )

    # Mount the adapter to the session
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
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
        
        print(f"Downloading {filename}")
#        print(f"Size: {file_size / (1024*1024):.2f} MB")
#        print(f"Saving to: {final_path}")

        # 4. Check for Multi-part Support
        # If server doesn't support ranges or size is unknown, fallback to single stream
        if file_size == 0 or accept_ranges == 'none':
            print("Server does not support resume/ranges. Switching to single connection.")
            max_concurrent_connections = 1
        else:
            max_concurrent_connections = min(max_concurrent_connections, int(file_size / min_chunk_size) + 1)

        # 5. Prepare File on Disk
        # Create empty file of specific size to allow random access writes
        with open(final_path, 'wb') as f:
            if file_size > 0:
                f.truncate(file_size)

        # 6. Calculate Ranges
        chunk_size = -(-file_size // max_concurrent_connections)
        map = SpaceMap(file_size)

        # 7. Start Download
        # unit_scale=True makes 1024 -> 1k, etc.
        with tqdm(total=file_size, unit='B', unit_scale=True, file=sys.stdout) as bar:
            with ThreadPoolExecutor(max_workers=max_concurrent_connections) as executor:
                futures = []
                param_dic = {}
                retries = 3
                while True:
                    while len(futures) < max_concurrent_connections:
                        next_range = map.get_next_available(chunk_size)
                        if not next_range:
                            if not param_dic:
                                break
                            key = max(param_dic, key=lambda k: param_dic[k][1] - param_dic[k][0])
                            param = param_dic[key]
                            length = (param[1] - param[0] + 1) // 2
                            if length < min_chunk_size:
                                break
                            end = param[1]
                            start = param[0] + length
                            param[1] = start - 1
                        else:
                            start, end = next_range
                        param = [start, end]
                        future = executor.submit(download_chunk, final_url, param, final_path, bar, session)
                        param_dic[future] = param
                        futures.append(future)
                        map.fill(start, end)
                    if not futures:
                        break
                    
                    for future in as_completed(futures):
                        futures.remove(future)
                        param_dic.pop(future)
                        rlt, start, end, e = future.result()
                        if not rlt:
                            map.vacant(start, end)
    #                        print("\nError occurred in one of the download threads.")
                            if retries <= 0:
                                raise e
                            retries -= 1
                        break
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