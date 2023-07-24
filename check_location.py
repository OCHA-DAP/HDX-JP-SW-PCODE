import logging
import re
from fiona import listlayers
from geopandas import read_file
from glob import glob
from os import mkdir
from os.path import basename, dirname, join
from pandas import isna, read_csv, read_excel
from shutil import rmtree
from zipfile import ZipFile, is_zipfile

from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.uuid import get_uuid

logger = logging.getLogger(__name__)


def get_global_pcodes(dataset_info, downloader, locations=list()):
    dataset = Dataset.read_from_hdx(dataset_info["dataset"])
    resource = [r for r in dataset.get_resources() if r["name"] == dataset_info["name"]]
    headers, iterator = downloader.get_tabular_rows(resource[0]["url"], dict_form=True)

    pcodes = {"WORLD": []}
    miscodes = {"WORLD": []}
    for row in iterator:
        pcode = row[dataset_info["p-code"]]
        iso3_code = row[dataset_info["admin"]]
        if len(locations) > 0 and iso3_code not in locations and "WORLD" not in locations:
            continue
        if iso3_code in pcodes:
            pcodes[iso3_code].append(pcode)
        else:
            pcodes[iso3_code] = [pcode]
        pcodes["WORLD"].append(pcode)

        iso2_code = Country.get_iso2_from_iso3(iso3_code)
        if iso3_code not in pcode and iso2_code not in pcode:
            continue

        pcode_no0 = pcode.replace("0", "")
        if iso3_code in miscodes:
            miscodes[iso3_code].append(pcode_no0)
        else:
            miscodes[iso3_code] = [pcode_no0]
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

    return pcodes, miscodes


def download_resource(resource, fileext, resource_folder):
    try:
        _, resource_file = resource.download(folder=resource_folder)
    except:
        error = f"Unable to download file"
        return None, error

    if fileext in ["xls", "xlsx"] and ".zip" not in basename(resource_file):
        resource_files = [resource_file]
        return resource_files, None

    if is_zipfile(resource_file) or ".zip" in basename(resource_file):
        temp = join(resource_folder, get_uuid())
        try:
            with ZipFile(resource_file, "r") as z:
                z.extractall(temp)
        except:
            error = f"Unable to unzip resource"
            return None, error
        resource_files = glob(join(temp, "**", f"*.{fileext}"), recursive=True)
        if len(resource_files) > 1:  # make sure to remove directories containing the actual files
            resource_files = [r for r in resource_files
                              if sum([r in rs for rs in resource_files if not rs == r]) == 0]
        if fileext == "xlsx" and len(resource_files) == 0:
            resource_files = [resource_file]
        if fileext in ["gdb", "gpkg"]:
            resource_files = [join(r, i) for r in resource_files for i in listlayers(r)]
    else:
        resource_files = [resource_file]

    return resource_files, None


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
        pcoded_header = any([bool(re.match(header_exp, head, re.IGNORECASE)) for head in headers])
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


def check_location(resource, pcodes, miscodes, temp_folder):
    pcoded = None
    mis_pcoded = None

    resource_folder = join(temp_folder, get_uuid())
    mkdir(resource_folder)

    filetype = resource.get_file_type()
    fileext = filetype
    if fileext == "geodatabase":
        fileext = "gdb"
    if fileext == "geopackage":
        fileext = "gpkg"

    resource_files, error = download_resource(resource, fileext, resource_folder)
    if not resource_files:
        rmtree(resource_folder)
        return None, None, error

    contents, error = read_downloaded_data(resource_files, fileext)

    if len(contents) == 0:
        rmtree(resource_folder)
        return None, None, error

    for key in contents:
        if pcoded:
            break
        pcoded = check_pcoded(contents[key], pcodes)

    if pcoded:
        rmtree(resource_folder)
        return pcoded, mis_pcoded, error

    for key in contents:
        if mis_pcoded:
            break
        mis_pcoded = check_pcoded(contents[key], miscodes, miscodes=True)

    if not error and pcoded is None:
        pcoded = False

    rmtree(resource_folder)
    return pcoded, mis_pcoded, error
