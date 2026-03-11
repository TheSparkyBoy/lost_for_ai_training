import shutil
import subprocess
import random
import csv
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
NUM_IMAGES = 1000     
PI_CORES = 4        # Adjust based on your Pi's capabilities
LOST_PATH = "./lost"  # Path to the compiled C++ executable
OUTPUT_DIR = "training_data"

# Default directories
RAW_DIR = os.path.join(OUTPUT_DIR, "raw")
ANNOTATED_DIR = os.path.join(OUTPUT_DIR, "annotated")

# Camera-specific main directories
OUTPUT_DIR_VST_68M = os.path.join(OUTPUT_DIR, "training_data_VST_68M")
OUTPUT_DIR_VST_41M = os.path.join(OUTPUT_DIR, "training_data_VST_41M")
OUTPUT_DIR_T3 = os.path.join(OUTPUT_DIR, "training_data_T3")
OUTPUT_DIR_APS = os.path.join(OUTPUT_DIR, "training_data_APS")
OUTPUT_DIR_CT_2020 = os.path.join(OUTPUT_DIR, "training_data_CT_2020")

# Group all the camera-specific directories into a list
camera_dirs = [
    OUTPUT_DIR_VST_68M, 
    OUTPUT_DIR_VST_41M, 
    OUTPUT_DIR_T3, 
    OUTPUT_DIR_APS, 
    OUTPUT_DIR_CT_2020
]

# --- CLEANUP STEP ---
# Check if the main folder already exists. If it does, obliterate it and everything inside.
if os.path.exists(OUTPUT_DIR):
    print(f"Clearing old dataset from '{OUTPUT_DIR}'...")
    shutil.rmtree(OUTPUT_DIR)

# 1. Create the default raw/annotated folders
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(ANNOTATED_DIR, exist_ok=True)

# 2. Loop through the cameras and create their nested folders
for cam_dir in camera_dirs:
    os.makedirs(os.path.join(cam_dir, "raw"), exist_ok=True)
    os.makedirs(os.path.join(cam_dir, "annotated"), exist_ok=True)

def generate_single_image_default(index):
    """Worker function to generate one image. Designed to run on a single CPU core."""
    ra = round(random.uniform(0, 360), 4)
    dec = round(random.uniform(-90, 90), 4)
    roll = round(random.uniform(0, 360), 4)
    
    base_name = f"star_field_{index:04d}.png"
    annotated_name = f"star_field_{index:04d}_annotated.png"
    raw_filepath = os.path.join(RAW_DIR, base_name)
    annotated_filepath = os.path.join(ANNOTATED_DIR, annotated_name)


    cmd = [
        LOST_PATH, "pipeline",
        "--generate", "1",
        "--generate-x-resolution", "3008",
        "--generate-y-resolution", "3008",
        "--fov", "30",
        
        # --- Brightness Controls ---
        "--generate-exposure", "1.5", 
        "--generate-zero-mag-photons", "80000",
        
        # --- Camera Orientation ---
        "--generate-ra", str(ra),
        "--generate-de", str(dec),
        "--generate-roll", str(roll),
        
        # --- Sensor Optics & Noise ---
        "--generate-spread-stddev", "1.5",
        "--generate-read-noise-stddev", "0.000",
        
        "--plot-raw-input", raw_filepath,
        "--plot-input", annotated_filepath
    ]

    my_env = os.environ.copy()
    my_env["ASAN_OPTIONS"] = "detect_leaks=0"
    
    try:
        # subprocess.run blocks the thread until finished, making it perfect for parallel execution
        subprocess.run(cmd, env=my_env, check=True, capture_output=True)
        print(f"[{index+1:04d}/{NUM_IMAGES:04d}] Generated {base_name} | RA: {ra:7.2f}, DEC: {dec:7.2f}")
        return [base_name, ra, dec, roll]
        
    except subprocess.CalledProcessError as e:
        print(f"Error on {base_name}: {e.stderr.decode()}")
        return None
    except FileNotFoundError:
        print(f"\nCRITICAL ERROR: Could not find '{LOST_PATH}'.")
        print("You must compile the C++ source code natively on the Pi 5 first!")
        return None

def generate_single_image_VST_68M(index):
    """Worker function to generate one image. Designed to run on a single CPU core."""
    ra = round(random.uniform(0, 360), 4)
    dec = round(random.uniform(-90, 90), 4)
    roll = round(random.uniform(0, 360), 4)
    
    base_name = f"star_field_{index:04d}.png"
    raw_filepath = os.path.join(OUTPUT_DIR_VST_68M, "raw", base_name)
    annotated_filepath = os.path.join(os.path.join(OUTPUT_DIR_VST_68M, "annotated"), f"star_field_{index:04d}_annotated.png")

    cmd = [
        LOST_PATH, "pipeline",
        "--generate", "1",
        "--generate-x-resolution", "1024",
        "--generate-y-resolution", "1024",
        "--fov", "14",
        
        # --- Brightness Controls ---
        "--generate-exposure", "0.2",
        "--generate-zero-mag-photons", "600000",
        
        # --- Camera Orientation ---
        "--generate-ra", str(ra),
        "--generate-de", str(dec),
        "--generate-roll", str(roll),
        
        # --- Sensor Optics & Noise ---
        "--generate-spread-stddev", "1.5",
        "--generate-read-noise-stddev", "0.000",
        
        "--plot-raw-input", raw_filepath,
        "--plot-input", annotated_filepath
    ]

    my_env = os.environ.copy()
    my_env["ASAN_OPTIONS"] = "detect_leaks=0"
    
    try:
        # subprocess.run blocks the thread until finished, making it perfect for parallel execution
        subprocess.run(cmd, env=my_env, check=True, capture_output=True)
        print(f"[{index+1:04d}/{NUM_IMAGES:04d}] Generated {base_name} | RA: {ra:7.2f}, DEC: {dec:7.2f}")
        return [base_name, ra, dec, roll]
        
    except subprocess.CalledProcessError as e:
        print(f"Error on {base_name}: {e.stderr.decode()}")
        return None
    except FileNotFoundError:
        print(f"\nCRITICAL ERROR: Could not find '{LOST_PATH}'.")
        print("You must compile the C++ source code natively on the Pi 5 first!")
        return None

def generate_single_image_VST_41M(index):
    """Worker function to generate one image. Designed to run on a single CPU core."""
    ra = round(random.uniform(0, 360), 4)
    dec = round(random.uniform(-90, 90), 4)
    roll = round(random.uniform(0, 360), 4)
    
    base_name = f"star_field_{index:04d}.png"
    raw_filepath = os.path.join(OUTPUT_DIR_VST_41M, "raw", base_name)
    annotated_filepath = os.path.join(os.path.join(OUTPUT_DIR_VST_41M, "annotated"), f"star_field_{index:04d}_annotated.png")

    cmd = [
        LOST_PATH, "pipeline",
        "--generate", "1",
        "--generate-x-resolution", "512",
        "--generate-y-resolution", "512",
        "--fov", "14",
        
        # --- Brightness Controls ---
        "--generate-exposure", "0.25", 
        "--generate-zero-mag-photons", "600000",
        
        # --- Camera Orientation ---
        "--generate-ra", str(ra),
        "--generate-de", str(dec),
        "--generate-roll", str(roll),
        
        # --- Sensor Optics & Noise ---
        "--generate-spread-stddev", "1.5",
        "--generate-read-noise-stddev", "0.000",
        
        "--plot-raw-input", raw_filepath,
        "--plot-input", annotated_filepath
    ]

    my_env = os.environ.copy()
    my_env["ASAN_OPTIONS"] = "detect_leaks=0"
    
    try:
        # subprocess.run blocks the thread until finished, making it perfect for parallel execution
        subprocess.run(cmd, env=my_env, check=True, capture_output=True)
        print(f"[{index+1:04d}/{NUM_IMAGES:04d}] Generated {base_name} | RA: {ra:7.2f}, DEC: {dec:7.2f}")
        return [base_name, ra, dec, roll]
        
    except subprocess.CalledProcessError as e:
        print(f"Error on {base_name}: {e.stderr.decode()}")
        return None
    except FileNotFoundError:
        print(f"\nCRITICAL ERROR: Could not find '{LOST_PATH}'.")
        print("You must compile the C++ source code natively on the Pi 5 first!")
        return None

def generate_single_image_T3(index):
    """Worker function to generate one image. Designed to run on a single CPU core."""
    ra = round(random.uniform(0, 360), 4)
    dec = round(random.uniform(-90, 90), 4)
    roll = round(random.uniform(0, 360), 4)
    
    base_name = f"star_field_{index:04d}.png"
    raw_filepath = os.path.join(OUTPUT_DIR_T3, "raw", base_name)
    annotated_filepath = os.path.join(os.path.join(OUTPUT_DIR_T3, "annotated"), f"star_field_{index:04d}_annotated.png")

    cmd = [
        LOST_PATH, "pipeline",
        "--generate", "1",
        "--generate-x-resolution", "1024",
        "--generate-y-resolution", "1024",
        "--fov", "20",
        
        # --- Brightness Controls ---
        "--generate-exposure", "0.2", 
        "--generate-zero-mag-photons", "600000",
        
        # --- Camera Orientation ---
        "--generate-ra", str(ra),
        "--generate-de", str(dec),
        "--generate-roll", str(roll),
        
        # --- Sensor Optics & Noise ---
        "--generate-spread-stddev", "1.5",
        "--generate-read-noise-stddev", "0.000",
        
        "--plot-raw-input", raw_filepath,
        "--plot-input", annotated_filepath
    ]

    my_env = os.environ.copy()
    my_env["ASAN_OPTIONS"] = "detect_leaks=0"
    
    try:
        # subprocess.run blocks the thread until finished, making it perfect for parallel execution
        subprocess.run(cmd, env=my_env, check=True, capture_output=True)
        print(f"[{index+1:04d}/{NUM_IMAGES:04d}] Generated {base_name} | RA: {ra:7.2f}, DEC: {dec:7.2f}")
        return [base_name, ra, dec, roll]
        
    except subprocess.CalledProcessError as e:
        print(f"Error on {base_name}: {e.stderr.decode()}")
        return None
    except FileNotFoundError:
        print(f"\nCRITICAL ERROR: Could not find '{LOST_PATH}'.")
        print("You must compile the C++ source code natively on the Pi 5 first!")
        return None

def generate_single_image_APS(index):
    """Worker function to generate one image. Designed to run on a single CPU core."""
    ra = round(random.uniform(0, 360), 4)
    dec = round(random.uniform(-90, 90), 4)
    roll = round(random.uniform(0, 360), 4)
    
    base_name = f"star_field_{index:04d}.png"
    raw_filepath = os.path.join(OUTPUT_DIR_APS, "raw", base_name)
    annotated_filepath = os.path.join(os.path.join(OUTPUT_DIR_APS, "annotated"), f"star_field_{index:04d}_annotated.png")

    cmd = [
        LOST_PATH, "pipeline",
        "--generate", "1",
        "--generate-x-resolution", "1024",
        "--generate-y-resolution", "1024",
        "--fov", "20",
        
        # --- Brightness Controls ---
        "--generate-exposure", "0.1", 
        "--generate-zero-mag-photons", "600000",
        
        # --- Camera Orientation ---
        "--generate-ra", str(ra),
        "--generate-de", str(dec),
        "--generate-roll", str(roll),
        
        # --- Sensor Optics & Noise ---
        "--generate-spread-stddev", "1.5",
        "--generate-read-noise-stddev", "0.000",
        
        "--plot-raw-input", raw_filepath,
        "--plot-input", annotated_filepath
    ]

    my_env = os.environ.copy()
    my_env["ASAN_OPTIONS"] = "detect_leaks=0"
    
    try:
        # subprocess.run blocks the thread until finished, making it perfect for parallel execution
        subprocess.run(cmd, env=my_env, check=True, capture_output=True)
        print(f"[{index+1:04d}/{NUM_IMAGES:04d}] Generated {base_name} | RA: {ra:7.2f}, DEC: {dec:7.2f}")
        return [base_name, ra, dec, roll]
        
    except subprocess.CalledProcessError as e:
        print(f"Error on {base_name}: {e.stderr.decode()}")
        return None
    except FileNotFoundError:
        print(f"\nCRITICAL ERROR: Could not find '{LOST_PATH}'.")
        print("You must compile the C++ source code natively on the Pi 5 first!")
        return None

def generate_single_image_CT_2020(index):
    """Worker function to generate one image. Designed to run on a single CPU core."""
    ra = round(random.uniform(0, 360), 4)
    dec = round(random.uniform(-90, 90), 4)
    roll = round(random.uniform(0, 360), 4)
    
    base_name = f"star_field_{index:04d}.png"
    raw_filepath = os.path.join(OUTPUT_DIR_CT_2020, "raw", base_name)
    annotated_filepath = os.path.join(os.path.join(OUTPUT_DIR_CT_2020, "annotated"), f"star_field_{index:04d}_annotated.png")

    cmd = [
        LOST_PATH, "pipeline",
        "--generate", "1",
        "--generate-x-resolution", "1024",
        "--generate-y-resolution", "1024",
        "--fov", "10",
        
        # --- Brightness Controls ---
        "--generate-exposure", "0.1", 
        "--generate-zero-mag-photons", "600000",
        
        # --- Camera Orientation ---
        "--generate-ra", str(ra),
        "--generate-de", str(dec),
        "--generate-roll", str(roll),
        
        # --- Sensor Optics & Noise ---
        "--generate-spread-stddev", "1.5",
        "--generate-read-noise-stddev", "0.000",
        
        "--plot-raw-input", raw_filepath,
        "--plot-input", annotated_filepath
    ]

    my_env = os.environ.copy()
    my_env["ASAN_OPTIONS"] = "detect_leaks=0"
    
    try:
        # subprocess.run blocks the thread until finished, making it perfect for parallel execution
        subprocess.run(cmd, env=my_env, check=True, capture_output=True)
        print(f"[{index+1:04d}/{NUM_IMAGES:04d}] Generated {base_name} | RA: {ra:7.2f}, DEC: {dec:7.2f}")
        return [base_name, ra, dec, roll]
        
    except subprocess.CalledProcessError as e:
        print(f"Error on {base_name}: {e.stderr.decode()}")
        return None
    except FileNotFoundError:
        print(f"\nCRITICAL ERROR: Could not find '{LOST_PATH}'.")
        print("You must compile the C++ source code natively on the Pi 5 first!")
        return None

if __name__ == "__main__":
    print(f"Starting generation of {NUM_IMAGES} images across {PI_CORES} Pi cores...")
    
    # Prepare CSV and execute parallel generation
    with open(os.path.join(OUTPUT_DIR, "metadata_default.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["filename", "ra", "dec", "roll"])
        
        # Spin up 4 worker threads to handle the subprocess calls
        with ThreadPoolExecutor(max_workers=PI_CORES) as executor:
            # Submit all image generation tasks to the pool
            futures = [executor.submit(generate_single_image_default, i) for i in range(NUM_IMAGES)]
            
            # As each image finishes generating on its core, write its metadata to the CSV
            for future in as_completed(futures):
                result = future.result()
                if result:
                    writer.writerow(result)

    print(f"\nDone! Dataset generated in '{RAW_DIR}'.")
    
    print(f"\nStarting generation (VECTRONIC VST-68M) of {NUM_IMAGES} images across {PI_CORES} Pi cores...")
    
    # Prepare CSV and execute parallel generation VST-68M
    with open(os.path.join(OUTPUT_DIR_VST_68M, "metadata_vst68m.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["filename", "ra", "dec", "roll"])
        
        # Spin up 4 worker threads to handle the subprocess calls
        with ThreadPoolExecutor(max_workers=PI_CORES) as executor:
            # Submit all image generation tasks to the pool
            futures = [executor.submit(generate_single_image_VST_68M, i) for i in range(NUM_IMAGES)]
            
            # As each image finishes generating on its core, write its metadata to the CSV
            for future in as_completed(futures):
                result = future.result()
                if result:
                    writer.writerow(result)

    print(f"\nDone! Dataset generated in '{OUTPUT_DIR_VST_68M}'.")

    print(f"\nStarting generation (VECTRONIC VST-41M) of {NUM_IMAGES} images across {PI_CORES} Pi cores...")

        # Prepare CSV and execute parallel generation VST-41M
    with open(os.path.join(OUTPUT_DIR_VST_41M, "metadata_vst41m.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["filename", "ra", "dec", "roll"])
        
        # Spin up 4 worker threads to handle the subprocess calls
        with ThreadPoolExecutor(max_workers=PI_CORES) as executor:
            # Submit all image generation tasks to the pool
            futures = [executor.submit(generate_single_image_VST_41M, i) for i in range(NUM_IMAGES)]
            
            # As each image finishes generating on its core, write its metadata to the CSV
            for future in as_completed(futures):
                result = future.result()
                if result:
                    writer.writerow(result)

    print(f"\nDone! Dataset generated in '{OUTPUT_DIR_VST_41M}'.")

    print(f"\nStarting generation (Terma T3 Star Tracker) of {NUM_IMAGES} images across {PI_CORES} Pi cores...")

            # Prepare CSV and execute parallel generation T3
    with open(os.path.join(OUTPUT_DIR_T3, "metadata_t3.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["filename", "ra", "dec", "roll"])
        
        # Spin up 4 worker threads to handle the subprocess calls
        with ThreadPoolExecutor(max_workers=PI_CORES) as executor:
            # Submit all image generation tasks to the pool
            futures = [executor.submit(generate_single_image_T3, i) for i in range(NUM_IMAGES)]
            
            # As each image finishes generating on its core, write its metadata to the CSV
            for future in as_completed(futures):
                result = future.result()
                if result:
                    writer.writerow(result)

    print(f"\nDone! Dataset generated in '{OUTPUT_DIR_T3}'.")

    print(f"\nStarting generation (Jena-Optronik ASTRO APS) of {NUM_IMAGES} images across {PI_CORES} Pi cores...")

        # Prepare CSV and execute parallel generation APS
    with open(os.path.join(OUTPUT_DIR_APS, "metadata_aps.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["filename", "ra", "dec", "roll"])
        
        # Spin up 4 worker threads to handle the subprocess calls
        with ThreadPoolExecutor(max_workers=PI_CORES) as executor:
            # Submit all image generation tasks to the pool
            futures = [executor.submit(generate_single_image_APS, i) for i in range(NUM_IMAGES)]
            
            # As each image finishes generating on its core, write its metadata to the CSV
            for future in as_completed(futures):
                result = future.result()
                if result:
                    writer.writerow(result)

    print(f"\nDone! Dataset generated in '{OUTPUT_DIR_APS}'.")

    print(f"\nStarting generation (BAE Systems CT-2020) of {NUM_IMAGES} images across {PI_CORES} Pi cores...")

        # Prepare CSV and execute parallel generation CT-2020
    with open(os.path.join(OUTPUT_DIR_CT_2020, "metadata_ct2020.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["filename", "ra", "dec", "roll"])
        
        # Spin up 4 worker threads to handle the subprocess calls
        with ThreadPoolExecutor(max_workers=PI_CORES) as executor:
            # Submit all image generation tasks to the pool
            futures = [executor.submit(generate_single_image_CT_2020, i) for i in range(NUM_IMAGES)]
            
            # As each image finishes generating on its core, write its metadata to the CSV
            for future in as_completed(futures):
                result = future.result()
                if result:
                    writer.writerow(result)

    print(f"\nDone! Dataset generated in '{OUTPUT_DIR_CT_2020}'.")
