import os
import shutil


def get_unique_filename(file_path):
    """Ensure the filename is unique to prevent overwriting existing files."""
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

    if not os.path.exists(file_path):
        return file_path

    base, ext = os.path.splitext(file_path)
    counter = 1
    new_path = f"{base}_{counter}{ext}"
    while os.path.exists(new_path):
        counter += 1
        new_path = f"{base}_{counter}{ext}"
    return new_path


def copy_and_rename_json(src_path, dest_path):
    """Copy and rename a JSON file to prevent overwriting."""
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"Source file does not exist: {src_path}")

    unique_dest = get_unique_filename(dest_path)
    shutil.copy2(src_path, unique_dest)
    print(f"✅ File has been copied and renamed to: {unique_dest}")
    
    
def delete_file(file_path):
    """Delete the file if it exists."""
    if os.path.exists(file_path):
        os.remove(file_path)


def get_market_country_based_on_url(url: str):
    config_dict = {
        "amazon.de": "Germany",
        "amazon.com": "United States",
        "meds.se": "Sweden",
        "apotea.se": "Sweden",
    }
    
    if url.lower() in config_dict:
        return config_dict[url]
    
    # Judge by domain name if not in config_dict
    # de = Germany, com = United States, se = Sweden
    # Extract the domain name from the URL
    domain_name = url.split(".")[-1]
    if domain_name in ["de", "com", "se"]:
        return config_dict[f"{domain_name.upper()}.{url.split('.')[-1]}"]
    return "Unknown" 
