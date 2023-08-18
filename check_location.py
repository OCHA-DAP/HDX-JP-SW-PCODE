import gzip
import logging
import re
from fiona import listlayers
from geopandas import read_file
from glob import glob
from os import mkdir, remove
from os.path import basename, dirname, join
from pandas import isna, read_csv, read_excel
from requests import head
from shutil import copyfileobj, rmtree
from zipfile import ZipFile, is_zipfile

from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.uuid import get_uuid
from helper.ckan import patch_resource_with_pcode_value

logger = logging.getLogger(__name__)


def get_global_pcodes(dataset_info, retriever, locations=None):
    dataset = Dataset.read_from_hdx(dataset_info["dataset"])
    resource = [r for r in dataset.get_resources() if r["name"] == dataset_info["name"]]
    headers, iterator = retriever.get_tabular_rows(resource[0]["url"], dict_form=True)

    pcodes = {"WORLD": []}
    miscodes = {"WORLD": []}
    for row in iterator:
        pcode = row[dataset_info["p-code"]]
        iso3_code = row[dataset_info["admin"]]
        if locations and len(locations) > 0 and iso3_code not in locations and "WORLD" not in locations:
            continue
        if iso3_code in pcodes:
            pcodes[iso3_code].append(pcode)
        else:
            pcodes[iso3_code] = [pcode]
        if iso3_code in miscodes:
            miscodes[iso3_code].append(pcode)
        else:
            miscodes[iso3_code] = [pcode]
        pcodes["WORLD"].append(pcode)

        iso2_code = Country.get_iso2_from_iso3(iso3_code)
        if iso3_code not in pcode and iso2_code not in pcode:
            continue

        pcode_no0 = pcode.replace("0", "")
        miscodes[iso3_code].append(pcode_no0)
        miscodes["WORLD"].append(pcode_no0)

        if iso3_code in pcode_no0:
            miscode = pcode_no0.replace(iso3_code, iso2_code)
            miscodes[iso3_code].append(miscode)
            miscodes["WORLD"].append(miscode)
            continue
        if iso2_code in pcode_no0:
            miscode = pcode_no0.replace(iso2_code, iso3_code)
            miscodes[iso3_code].append(miscode)
            miscodes["WORLD"].append(miscode)

    for iso in miscodes:
        miscodes[iso] = list(set(miscodes[iso]))
        miscodes[iso].sort()

    return pcodes, miscodes


def download_resource(resource, fileext, retriever):
    try:
        resource_file = retriever.download_file(resource["url"])
    except:
        error = f"Unable to download file"
        return None, None, error

    if fileext in ["xls", "xlsx"] and ".zip" not in basename(resource_file):
        resource_files = [resource_file]
        return resource_files, None, None

    if is_zipfile(resource_file) or ".zip" in basename(resource_file) or ".gz" in basename(resource_file):
        parent_folder = join(retriever.temp_dir, get_uuid())
        parent_folders = [parent_folder, resource_file]
        if ".gz" in basename(resource_file):
            try:
                mkdir(parent_folder)
                with gzip.open(resource_file, "rb") as gz:
                    with open(join(parent_folder, basename(resource_file.replace(".gz", ".gpkg"))), "wb") as gz_out:
                        copyfileobj(gz, gz_out)
            except:
                error = f"Unable to unzip resource"
                return None, parent_folders, error
        else:
            try:
                with ZipFile(resource_file, "r") as z:
                    z.extractall(parent_folder)
            except:
                error = f"Unable to unzip resource"
                return None, parent_folders, error
        resource_files = glob(join(parent_folder, "**", f"*.{fileext}"), recursive=True)
        if len(resource_files) > 1:  # make sure to remove directories containing the actual files
            resource_files = [r for r in resource_files
                              if sum([r in rs for rs in resource_files if not rs == r]) == 0]
        if fileext == "xlsx" and len(resource_files) == 0:
            resource_files = [resource_file]
        if fileext in ["gdb", "gpkg"]:
            resource_files = [join(r, i) for r in resource_files for i in listlayers(r)]

    elif fileext in ["gdb", "gpkg"] and ".zip" not in basename(resource_file) and ".gz" not in basename(resource_file):
        resource_files = [join(resource_file, r) for r in listlayers(resource_file)]
        parent_folders = [resource_file]

    else:
        resource_files = [resource_file]

    return resource_files, parent_folders, None


def read_downloaded_data(resource_files, fileext):
    data = dict()
    error = None
    for resource_file in resource_files:
        if fileext in ["xlsx", "xls"]:
            try:
                contents = read_excel(
                    resource_file, sheet_name=None, nrows=200
                )
            except:
                error = f"Unable to read resource"
                continue
            for key in contents:
                if contents[key].empty:
                    continue
                data[get_uuid()] = parse_tabular(contents[key], fileext)
        if fileext == "csv":
            try:
                contents = read_csv(resource_file, nrows=200, skip_blank_lines=True)
                data[get_uuid()] = parse_tabular(contents, fileext)
            except:
                error = f"Unable to read resource"
                continue
        if fileext in ["geojson", "json", "shp", "topojson"]:
            try:
                data = {
                    get_uuid(): read_file(resource_file, rows=200)
                }
            except:
                error = f"Unable to read resource"
                continue
        if fileext in ["gdb", "gpkg"]:
            try:
                data = {
                    get_uuid(): read_file(dirname(resource_file), layer=basename(resource_file), rows=200)
                }
            except:
                error = f"Unable to read resource"
                continue

    return data, error


def parse_tabular(df, fileext):
    df = df.dropna(how="all", axis=0).dropna(how="all", axis=1).reset_index(drop=True)
    df.columns = [str(c) for c in df.columns]
    if all([bool(re.match("Unnamed.*", c)) for c in df.columns]):  # if all columns are unnamed, move down a row
        df.columns = [str(c) if not isna(c) else f"Unnamed: {i}" for i, c in enumerate(df.loc[0])]
        df = df.drop(index=0).reset_index(drop=True)
    if not all(df.dtypes == "object"):  # if there are mixed types, probably read correctly
        return df
    if len(df) == 1:  # if there is only one row, return
        return df
    hxlrow = None  # find hxl row and incorporate into header
    i = 0
    while i < 10 and i < len(df) and hxlrow is None:
        hxltags = [bool(re.match("#.*", t)) if t else True for t in df.loc[i].astype(str)]
        if all(hxltags):
            hxlrow = i
        i += 1
    if hxlrow is not None:
        columns = []
        for c in df.columns:
            cols = [str(col) for col in df[c][:hxlrow + 1] if col]
            if "Unnamed" not in c:
                cols = [c] + cols
            columns.append("||".join(cols))
        df.columns = columns
        df = df.drop(index=range(hxlrow + 1)).reset_index(drop=True)
        return df
    if fileext == "csv" and not hxlrow:  # assume first row of csv is header if there are no hxl tags
        return df
    columns = []
    datarow = 3
    if hxlrow:
        datarow = hxlrow + 1
    if len(df) < 3:
        datarow = len(df)
    for c in df.columns:
        cols = [str(col) for col in df[c][:datarow] if col]
        if "Unnamed" not in c:
            cols = [c] + cols
        columns.append("||".join(cols))
    df.columns = columns
    df = df.drop(index=range(datarow)).reset_index(drop=True)
    return df


def check_pcoded(df, pcodes, miscodes=False):
    pcoded = None
    header_exp = "((adm)?.*p?.?cod.*)|(#\s?adm\s?\d?\+?\s?p?(code)?)"

    for h in df.columns:
        if pcoded:
            break
        headers = h.split("||")
        pcoded_header = any([bool(re.match(header_exp, hh, re.IGNORECASE)) for hh in headers])
        if not pcoded_header:
            continue
        column = df[h].dropna().astype("string").str.upper()
        column = column[~column.isin(["NA", "NAN", "NONE", "NULL", ""])]
        if len(column) == 0:
            continue
        if miscodes:
            column = column.str.replace("0", "")
        matches = sum(column.isin(pcodes))
        pcnt_match = matches / len(column)
        if pcnt_match >= 0.9:
            pcoded = True

    return pcoded


def remove_files(files=None, folders=None):
    if files:
        to_delete = files
        if folders:
            to_delete = files + folders
    elif folders:
        to_delete = folders
    for f in to_delete:
        try:
            remove(f)
        except (FileNotFoundError, NotADirectoryError, TypeError):
            pass
        try:
            rmtree(f)
        except (FileNotFoundError, NotADirectoryError, TypeError):
            pass


def process_resource(resource, dataset, global_pcodes, global_miscodes, retriever, configuration, update=True):
    pcoded = None
    mis_pcoded = None

    locations = dataset.get_location_iso3s()
    pcodes = [pcode for iso in global_pcodes for pcode in global_pcodes[iso] if iso in locations]
    miscodes = [pcode for iso in global_miscodes for pcode in global_miscodes[iso] if iso in locations]

    filetype = resource.get_file_type()
    fileext = filetype
    if fileext == "geodatabase":
        fileext = "gdb"
    if fileext == "geopackage":
        fileext = "gpkg"

    if dataset.get_organization()["name"] in configuration["org_exceptions"]:
        pcoded = False

    if filetype.lower() not in configuration["allowed_filetypes"]:
        pcoded = False

    if pcoded is None:
        size = resource["size"]
        if (size is None or size == 0) and resource["resource_type"] == "api":
            try:
                resource_info = head(resource["url"])
                # if size cannot be determined, set to the limit set in configuration so the resource is excluded
                size = int(resource_info.headers.get("Content-Length", configuration["resource_size"]))
            except:
                size = configuration["resource_size"]

        if size >= configuration["resource_size"]:
            pcoded = False

    if pcoded is False:
        return pcoded, mis_pcoded

    resource_files, parent_folders, error = download_resource(resource, fileext, retriever)
    if not resource_files:
        remove_files(folders=parent_folders)
        if error:
            logger.error(f"{dataset['name']}: {resource['name']}: {error}")
        return None, None

    contents, error = read_downloaded_data(resource_files, fileext)

    if len(contents) == 0:
        remove_files(resource_files, parent_folders)
        if error:
            logger.error(f"{dataset['name']}: {resource['name']}: {error}")
        return None, None

    for key in contents:
        if pcoded:
            break
        pcoded = check_pcoded(contents[key], pcodes)

    if pcoded:
        remove_files(resource_files, parent_folders)
        if error:
            logger.error(f"{dataset['name']}: {resource['name']}: {error}")
        return pcoded, mis_pcoded

    for key in contents:
        if mis_pcoded:
            break
        mis_pcoded = check_pcoded(contents[key], miscodes, miscodes=True)

    if not error and pcoded is None:
        pcoded = False

    if mis_pcoded:
        logger.warning(f"{dataset['name']}: {resource['name']}: may be mis-pcoded")

    if error:
        logger.error(f"{dataset['name']}: {resource['name']}: {error}")

    remove_files(resource_files, parent_folders)

    if update:
        try:
            patch_resource_with_pcode_value(resource['id'], pcoded)
        except Exception:
            logger.exception(f"Could not update resource {resource['id']} in dataset {dataset['name']}")
            raise

    return pcoded, mis_pcoded
