import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from threading import Thread, Event
import os
import zipfile
import requests
import pandas as pd
from lxml import etree
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# === Global state ===
cancel_flag = Event()
last_extract_dir = None

def parse_xml_file(file_path: str) -> list[dict]:
    tree = etree.parse(file_path)
    root = tree.getroot()

    ns = {'g': 'http://www.fda.gov/cdrh/gudid'}

    results = []
    devices = root.findall('.//g:device', namespaces=ns)
    print(f"[DEBUG] Found {len(devices)} devices in: {file_path}")

    for device in devices:
        def find_text(path):
            element = device.find(path, namespaces=ns)
            return element.text.strip() if element is not None and element.text else None

        row = {
            'deviceId': find_text('g:identifiers/g:identifier/g:deviceId'),
            'versionModelNumber': find_text('g:versionModelNumber'),
            'catalogNumber': find_text('g:catalogNumber'),
            'dunsNumber': find_text('g:dunsNumber'),
            'companyName': find_text('g:companyName'),
            'deviceDescription': find_text('g:deviceDescription'),
            'singleUse': find_text('g:singleUse'),
            'lotBatch': find_text('g:lotBatch'),
            'serialNumber': find_text('g:serialNumber')
        }

        results.append(row)

    return results

def write_split_csv(rows, output_path):
    base, ext = os.path.splitext(output_path)
    chunk_size = 500000
    total_rows = len(rows)
    num_chunks = (total_rows // chunk_size) + (1 if total_rows % chunk_size else 0)

    for i in range(num_chunks):
        chunk = rows[i * chunk_size : (i + 1) * chunk_size]
        df = pd.DataFrame(chunk)
        if num_chunks == 1:
            filename = output_path
        else:
            filename = f"{base}_{i+1}{ext}"
        df.to_csv(filename, index=False)
        logging.info("Wrote %d rows to: %s", len(chunk), filename)

input_mode = None
local_zip_path = None


def download_and_process_zip(url, local_path, mode, extract_dir, output_csv, progressbar, status_label):
    try:
        cancel_flag.clear()

        # === Log file setup ===
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = os.path.join(extract_dir, f"GreatbatchLog_{timestamp}.log")
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logging.info("=== Starting processing ===")

        if mode == "download":
            set_status(status_label, "Downloading ZIP...")
            logging.info("Download URL: %s", url)
            progressbar.config(mode='determinate')
            zip_path = os.path.join(extract_dir, "downloaded.zip")

            with requests.get(url, stream=True) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                chunk_size = 8192
                progressbar["maximum"] = total_size

                with open(zip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if cancel_flag.is_set():
                            set_status(status_label, "Cancelled during download")
                            logging.warning("Cancelled during download")
                            return
                        f.write(chunk)
                        downloaded += len(chunk)
                        progressbar["value"] = downloaded
                        progressbar.update_idletasks()
                        set_status(status_label, f"Downloading... {downloaded / (1024*1024):.2f} MB / {total_size / (1024*1024):.2f} MB")
        else:
            zip_path = local_path
            if not os.path.isfile(zip_path):
                set_status(status_label, "Invalid local ZIP path")
                logging.error("Invalid local ZIP path: %s", zip_path)
                return
            logging.info("Using local ZIP file: %s", zip_path)
            set_status(status_label, f"Using local file: {os.path.basename(zip_path)}")

        # === Extract ZIP ===
        set_status(status_label, "Extracting ZIP...")
        logging.info("Extracting ZIP")
        total_unzipped_size = 0
        progressbar.config(mode='indeterminate')
        progressbar.start()

        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
                for file_info in zip_ref.infolist():
                    total_unzipped_size += file_info.file_size
        except Exception as e:
            logging.exception("Error during ZIP extraction")
            set_status(status_label, "Extraction failed")
            return

        progressbar.stop()
        set_status(status_label, f"Extracted: {total_unzipped_size / (1024 * 1024):.2f} MB")
        logging.info("Extraction complete: %.2f MB", total_unzipped_size / (1024 * 1024))

        # === Parse XML files ===
        set_status(status_label, "Parsing XML files...")
        files = [f for f in os.listdir(extract_dir) if f.endswith(".xml")]
        total = len(files)
        progressbar.config(mode='determinate', maximum=total, value=0)
        logging.info("Found %d XML files", total)
        rows = []

        def worker(file_path):
            if cancel_flag.is_set():
                return []
            return parse_xml_file(file_path)

        with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as executor:
            futures = {
                executor.submit(worker, os.path.join(extract_dir, f)): f for f in files
            }

            completed = 0
            for future in as_completed(futures):
                if cancel_flag.is_set():
                    set_status(status_label, "Cancelled during parsing")
                    logging.warning("Cancelled during parsing")
                    return
                try:
                    result = future.result()
                    rows.extend(result)
                    logging.info("Parsed file: %s", futures[future])
                except Exception as e:
                    logging.exception("Error parsing file: %s", futures[future])
                completed += 1
                progressbar["value"] = completed
                progressbar.update_idletasks()
                set_status(status_label, f"Parsed {completed}/{total} files")

        # === Output CSV ===
        set_status(status_label, "Writing CSV...")
        try:
            write_split_csv(rows, output_csv)
        except Exception as e:
            logging.exception("Error writing CSV")
            set_status(status_label, "CSV write failed")
            return

        set_status(status_label, f"Done! Saved to {output_csv}")
        root.after(0, lambda: messagebox.showinfo("Done", f"CSV written to: {output_csv}"))
        logging.info("=== Processing complete ===")

    except Exception as e:
        progressbar.stop()
        set_status(status_label, "Error")
        logging.exception("Processing failed")
        messagebox.showerror("Error", str(e))

def run_processing():
    global last_extract_dir
    url = url_entry.get()
    base_folder = filedialog.askdirectory(title="Choose Working Folder")
    output = filedialog.asksaveasfilename(defaultextension=".csv", title="Save Output CSV As")
    if base_folder and output:
        extract_folder = os.path.join(base_folder, "extract")
        os.makedirs(extract_folder, exist_ok=True)
        last_extract_dir = extract_folder
        cleanup_btn.config(state=tk.NORMAL)
        Thread(target=download_and_process_zip, args=(
            url, local_zip_path.get(), input_mode.get(), extract_folder, output, progress, status_label)).start()

def cancel_process():
    cancel_flag.set()
    set_status(status_label, "Cancelling...")

def clean_up():
    if not last_extract_dir or not os.path.isdir(last_extract_dir):
        messagebox.showwarning("Cleanup", "No extract directory available. Run the tool first.")
        return

    deleted = 0
    for file in os.listdir(last_extract_dir):
        if file == "downloaded.zip" or file.endswith(".xml"):
            try:
                os.remove(os.path.join(last_extract_dir, file))
                deleted += 1
            except Exception as e:
                logging.exception("Error deleting file: %s", file)
    messagebox.showinfo("Cleanup", f"Deleted {deleted} file(s) from:\n{last_extract_dir}")

def set_status(label, text):
    label.config(text=text)
    label.update_idletasks()

def show_credits():
    messagebox.showinfo(
        "Credits",
        "This application uses the following open source libraries:\n\n"
        "- requests (Apache 2.0)\n"
        "- pandas (BSD License)\n"
        "- lxml (BSD License)\n"
        "- tkinter (Python Standard Library)\n\n"
        "Developed by Oscar G.C. (Ozkr16) and Johan C.A.\n\n"
        "The name 'Greatbatch' honors Wilson Greatbatch, "
        "inventor of the implantable cardiac pacemaker."
    )

# === GUI Setup ===
root = tk.Tk()

input_mode = tk.StringVar(value="download")
local_zip_path = tk.StringVar()

root.title("Greatbatch Device Data Processor by Oscar G.C. (Ozkr16) and Johan C.A.")
root.geometry("580x430")
root.resizable(False, False)

tk.Label(root, text="ZIP File URL:").pack(pady=(10, 0))
url_entry = tk.Entry(root, width=70)
url_entry.pack(pady=(0, 10))

tk.Label(root, text="Input Mode:").pack()
mode_frame = tk.Frame(root)
mode_frame.pack()
tk.Radiobutton(mode_frame, text="Download from URL", variable=input_mode, value="download", command=lambda: local_file_btn.config(state=tk.DISABLED)).pack(side=tk.LEFT)
tk.Radiobutton(mode_frame, text="Use Local ZIP File", variable=input_mode, value="local", command=lambda: local_file_btn.config(state=tk.NORMAL)).pack(side=tk.LEFT)

def choose_local_file():
    path = filedialog.askopenfilename(filetypes=[("ZIP files", "*.zip")])
    if path:
        local_zip_path.set(path)

local_file_btn = tk.Button(root, text="Choose Local ZIP File", command=choose_local_file, state=tk.DISABLED)
local_file_btn.pack(pady=(5, 10))

tk.Button(root, text="Run", width=30, command=run_processing).pack(pady=(0, 5))
tk.Button(root, text="Cancel", width=30, command=cancel_process).pack(pady=(0, 5))

cleanup_btn = tk.Button(root, text="Clean Up ZIP/XML", width=30, command=clean_up, state=tk.DISABLED)
cleanup_btn.pack(pady=(0, 5))

tk.Button(root, text="Credits", width=30, command=show_credits).pack(pady=(0, 10))

progress = ttk.Progressbar(root, length=480, mode='indeterminate')
progress.pack(pady=(0, 10))

status_label = tk.Label(root, text="Idle", anchor='center')
status_label.pack(pady=(0, 10))

root.mainloop()
