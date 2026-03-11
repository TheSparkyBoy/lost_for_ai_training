import subprocess
import random
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
NUM_IMAGES = 200     # The total number of images you want to generate
OUTPUT_DIR = "training_data"
RAW_DIR = os.path.join(OUTPUT_DIR, "raw")
ANNOTATED_DIR = os.path.join(OUTPUT_DIR, "annotated")
LOST_PATH = "./lost"  # Ensure this is the newly compiled ARM64 binary!
PI_CORES = 4          # The Raspberry Pi 5 has a quad-core CPU

# Create output directories
for directory in [OUTPUT_DIR, RAW_DIR, ANNOTATED_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

def generate_single_image(index):
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

def generate_single_imag_VST_68M(index):
    """Worker function to generate one image. Designed to run on a single CPU core."""
    ra = round(random.uniform(0, 360), 4)
    dec = round(random.uniform(-90, 90), 4)
    roll = round(random.uniform(0, 360), 4)
    
    base_name = f"star_field_{index:04d}.png"
    raw_filepath = os.path.join(RAW_DIR, base_name)

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
        
        "--plot-raw-input", raw_filepath
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
    raw_filepath = os.path.join(RAW_DIR, base_name)

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
        
        "--plot-raw-input", raw_filepath
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
    raw_filepath = os.path.join(RAW_DIR, base_name)

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
        
        "--plot-raw-input", raw_filepath
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
    raw_filepath = os.path.join(RAW_DIR, base_name)

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
        
        "--plot-raw-input", raw_filepath
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
    raw_filepath = os.path.join(RAW_DIR, base_name)

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
        
        "--plot-raw-input", raw_filepath
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
    with open(os.path.join(OUTPUT_DIR, "metadata.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["filename", "ra", "dec", "roll"])
        
        # Spin up 4 worker threads to handle the subprocess calls
        with ThreadPoolExecutor(max_workers=PI_CORES) as executor:
            # Submit all image generation tasks to the pool
            futures = [executor.submit(generate_single_image, i) for i in range(NUM_IMAGES)]
            
            # As each image finishes generating on its core, write its metadata to the CSV
            for future in as_completed(futures):
                result = future.result()
                if result:
                    writer.writerow(result)

    print(f"\nDone! Dataset generated in '{RAW_DIR}'.")
