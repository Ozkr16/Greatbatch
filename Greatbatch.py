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
    results = []
    devices = root.findall('./device')

    for device in devices:
        def find_text(path):
            element = device.find(path)
            return element.text.strip() if element is not None and element.text else None

        row = {
            'deviceId': find_text('./identifiers/identifier/deviceId'),
            'versionModelNumber': find_text('./versionModelNumber'),
            'catalogNumber': find_text('./catalogNumber/Element'),
            'dunsNumber': find_text('./dunsNumber'),
            'companyNumber': find_text('./companyNumber'),
            'deviceDescription': find_text('./deviceDescription/Element'),
            'singleUse': find_text('./singleUse'),
            'lotBatch': find_text('./lotBatch'),
            'serialNumber': find_text('./serialNumber')
        }

        results.append(row)

    return results

def download_and_process_zip(url, extract_dir, output_csv, progressbar, status_label):
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
        logging.info("Download URL: %s", url)

        # === Download ZIP ===
        set_status(status_label, "Downloading ZIP...")
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

        # === Extract ZIP ===
        set_status(status_label, "Extracting ZIP...")
        logging.info("Extracting ZIP")
        total_unzipped_size = 0
        progressbar.config(mode='indeterminate')
        progressbar.start()

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
            for file_info in zip_ref.infolist():
                total_unzipped_size += file_info.file_size

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
        logging.info("Writing CSV to: %s", output_csv)
        df = pd.DataFrame(rows)
        df.to_csv(output_csv, index=False)
        logging.info("CSV write complete")

        set_status(status_label, f"Done! Saved to {output_csv}")
        messagebox.showinfo("Done", f"CSV written to: {output_csv}")
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
    if url and base_folder and output:
        extract_folder = os.path.join(base_folder, "extract")
        os.makedirs(extract_folder, exist_ok=True)
        last_extract_dir = extract_folder
        cleanup_btn.config(state=tk.NORMAL)
        Thread(target=download_and_process_zip, args=(
            url, extract_folder, output, progress, status_label)).start()

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
                print(f"Error deleting {file}: {e}")
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
        "🫀 The name 'Greatbatch' honors Wilson Greatbatch, "
        "inventor of the implantable cardiac pacemaker."
    )

# === GUI Setup ===
root = tk.Tk()
root.title("Greatbatch Device Data Processor by Oscar G.C. (Ozkr16) and Johan C.A.")
root.geometry("540x380")
root.resizable(False, False)

tk.Label(root, text="ZIP File URL:").pack(pady=(10, 0))
url_entry = tk.Entry(root, width=70)
url_entry.pack(pady=(0, 10))

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
