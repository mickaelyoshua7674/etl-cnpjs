from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from zipfile import ZipFile
import wget, os, json, traceback, re
import pandas as pd

# CHROMEDRIVER_PATH = "/usr/lib/chromium-browser/chromedriver"
# URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
# ZIPFILES_DIR = "zipfiles"
# if not os.path.exists(ZIPFILES_DIR):
#     os.mkdir(ZIPFILES_DIR)

RAWFILES_DIR = "rawfiles"
if not os.path.exists(RAWFILES_DIR):
    os.mkdir(RAWFILES_DIR)

# op = webdriver.ChromeOptions()
# op.add_argument("log-level=3") # https://stackoverflow.com/questions/46744968/how-to-suppress-console-error-warning-info-messages-when-executing-selenium-pyth
# op.add_argument("headless") # don't open a Chrome window
# sc = Service(CHROMEDRIVER_PATH)
# driver = webdriver.Chrome(service=sc, options=op)

# driver.get(URL)
# tr_elements = [e for e in driver.find_elements(By.TAG_NAME, "tr")]
# zipfiles_list =  [e.text.split(" ")[0] for e in tr_elements[2:] if ".zip" in e.text]
# driver.quit()

# for zf in zipfiles_list:
#     full_zip_path = os.path.join(ZIPFILES_DIR, zf)

#     print(f"Downloading {zf}...")
#     wget.download(URL + zf, ZIPFILES_DIR)
#     print("\n\n")

#     print(f"Extracting {zf}...")
#     with ZipFile(full_zip_path, "r") as z:
#         for i in z.infolist():
#             i.filename = zf.split(".")[0] + ".csv"
#             z.extract(i, RAWFILES_DIR)
#     print(f"{zf} Extracted.\n\n")
#     os.remove(full_zip_path)
# os.rmdir(ZIPFILES_DIR)

MERGED_DIR = "mergedfiles"
if not os.path.exists(MERGED_DIR):
    os.mkdir(MERGED_DIR)

with open("table_file.json", "r") as f:
    table_file = json.load(f)
with open("all_dtypes.json", "r") as f:
    dtypes = json.load(f)

for tf in table_file:
    print(f"Merging {tf[1]}...")
    number_files = len([f for f in os.listdir(RAWFILES_DIR) if f.startswith(tf[1])])
    if number_files > 1:
        header = True
        for i in range(number_files):
            with open(os.path.join(RAWFILES_DIR, tf[1]+str(i)+".csv"), "r", encoding="latin-1") as f:
                lines = f.readlines()
            for j, line in enumerate(lines):
                data = [d for d in re.sub('( "")|("" )|(""")', "", line.strip()).split('"')[1:-1] if d != ";"]
                try:
                    pd.DataFrame(data=[data], columns=[k for k in dtypes[tf[0]].keys()])\
                    .to_csv(os.path.join(MERGED_DIR, tf[1]+".csv"), header=header, index=False, mode="a")
                    header = False
                except:
                    print(f"\n\n{i}\n{j+1}\n{line}\n\n")
                    traceback.print_exc()
                    exit()
    else:
        header = True
        with open(os.path.join(RAWFILES_DIR, tf[1]+".csv"), "r", encoding="latin-1") as f:
            lines = f.readlines()
        for i, line in enumerate(lines):
            data = [d for d in re.sub('( "")|("" )|(""")|([A-Z]""[A-Z])', "", line.strip()).split('"')[1:-1] if d != ";"]
            try:
                pd.DataFrame(data=[data], columns=[k for k in dtypes[tf[0]].keys()])\
                .to_csv(os.path.join(MERGED_DIR, tf[1]+".csv"), header=header, index=False, mode="a")
                header = False
            except:
                print(f"\n\n{i+1}\n{line}\n\n")
                traceback.print_exc()
                exit()
            


