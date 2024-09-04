"""This modules implements utility functions to download and load GHSL data.

Primarly thought as a companion to the citylims package.
While the citylims package can work with any population grid, data from the
Global Human Settlement project is a harmonized global dataset from wich the
official Degree of Urbanization classificaction is obtained.
This package is created for covenience and to allow for testing and comparison among
methods.

Inspired by functionallity of the Flexurba R package,
reimplements the download functions for GHSL data.

Supports donwloading and loading the followind data products of the 2023A release 
from the GHSL repositories.

- BUILT-S: built-up surface.
- POP: population grid.
- LAND: permanent land binary raster.

This products are available in two resolutions, 100 and 1000 meters, and in two
projections, Mollweide (54009) and WGS84 (4326).
Documentation for the GHSL data products is available at
https://ghsl.jrc.ec.europa.eu/download.php.

"""

import itertools
import shutil
from importlib.resources import as_file, files
from pathlib import Path
from urllib.request import urlretrieve
from zipfile import ZipFile

import geopandas as gpd
from osgeo import gdal

VALID_PRODS = ("BUILT_S", "POP", "LAND", "SMOD")
VALID_EPOCHS = tuple(range(1975, 2031, 5))


def load_ghsl_tiles():
    """Loads GHSL tiles geodataframe.

    The dataframe contains the geometry and metadata of valid tiles.
    A region columns have been added with one of 9 regions.
    These regions are the smallest possible grouping of GHSL tiles,
    ensuring that no continuous land mass is split across two regions.

    The 9 regions cover approximately the following areas:
        - W_AEA: Asia - Europe - Africa - Oceania (eastern hemisphere)
        - W_AME:  North and South America (+ Greenland and Iceland)
        - W_ISL1:  Hawaii
        - W_ISL2:  Oceanic Islands (western hemisphere)
        - W_ISL3:  Chatham Islands
        - W_ISL4:  Scott Island
        - W_ISL5:  Saint-Helena, Ascension and Tristan da Cunha
        - W_ISL6:  French Southern and Antarctic Lands
        - W_ANT: Antarctica

    Returns
    -------
    GeoDataFrame
        GeoDataFrame with GHSL global tiles in Mollweide projection.
    """
    with as_file(files("ghsldownloader") / "data/GHSL_tiles.gpkg") as f:
        gdf = gpd.read_file(f)
    return gdf


def download_ghsl(
    output_dir,
    products=("POP", "BUILT_S", "LAND", "SMOD"),
    epochs=(2020,),
    crs=54009,
    resolution=1000,
    extent="global",
    prefix="",
    bbox=None,
    tiles=None,
):
    """Downloads GHSL data products.

    Parameters
    ----------
    output_dir : Path
        Path to directory to store downloaded products.
    products : tuple, optional
        Tuple with the list of products to download,
        by default ("POP", "BUILT-S", "LAND")
    epochs : tuple, optional
        Tuple with the epochs to download, by default (2020)
    crs : int, optional
        Projection of the data products, by default 54009
    resolution : int, optional
        Resolution in meters of the data products, by default 1000
    extent : str, optional
        Extent of the products to download, can be one of , by default "global".
        Can be one of "global", "regions", "bbox", or "tiles".
        "global" downloads all tiles available at a global scale.
        "regions" downloads all tiles, but split into 9 regions for easier processing.
        "bbox" downloads all tiles intersecting a bounding box given in bbox.
        "tiles" downloads tiles specified in tiles argument.
    prefix : str, optional
        Prefix to append to the filenames of the downloaded products,
        by default an empty string.

    Returns
    -------
    List
        List of paths to downloaded products.

    """

    output_dir = Path(output_dir)

    if extent not in ["global", "regions", "tiles", "bbox"]:
        raise ValueError("Unsupported extent argument.")

    # If director does not exists, create it
    output_dir.mkdir(parents=True, exist_ok=True)

    # Check inputs
    for product in products:
        if product not in VALID_PRODS:
            raise ValueError(f"{product} not a valid product.")
    for epoch in epochs:
        if epoch not in VALID_EPOCHS:
            raise ValueError(f"{epoch} not a valid epoch.")
    if crs not in (4326, 54009):
        raise ValueError(f"{crs} not a valid crs.")
    if resolution not in (100, 1000):
        raise ValueError(f"{resolution} not a valid resolution.")

    # Check SMOS resolution
    if "SMOD" in products and resolution == 100:
        raise ValueError("SMOD is only available at 1000 resolution.")
    # If LAND is requested, crs must be mollweide
    if "LAND" in products and crs == 4326:
        raise ValueError("LAND is only available in crs 54009.")

    # Generate combination of parameters
    # Land is a special case, since its only available for 2018 Mollweide
    prod_list = []
    if "LAND" in products:
        products = (p for p in products if p != "LAND")
        prod_list.append(("LAND", 2018, 54009, resolution))
    prod_list.extend(itertools.product(products, epochs, [crs], [resolution]))

    # Get a list of tiles to download
    if extent == "global":
        # We will request the global file
        tile_id = ("global",)
    elif extent == "regions":
        # We will request all tiles, split into regions
        # We will download all tiles and merge them afterwards
        tiles_gdf = load_ghsl_tiles()
        regions_dict = tiles_gdf.groupby("region").tile_id.apply(tuple).to_dict()
        tile_id = tiles_gdf.tile_id.values
    elif extent == "tiles":
        # Just get the tile list
        # Check tiles are valid tiles
        tiles_gdf = load_ghsl_tiles()
        for tile in tiles:
            if tile not in tiles_gdf.tile_id.values:
                raise ValueError(f"{tile} not a valid tile.")
        tile_id = tiles
    elif extent == "bbox":
        tiles_gdf = load_ghsl_tiles()
        tile_id = tiles_gdf[tiles_gdf.intersects(bbox)].tile_id.values

    # Create download urls for each product
    product_dict = {}
    for product, epoch, crs, resolution in prod_list:
        curr_prod_dict = {"tiles": {}}
        product_dict[(product, epoch, crs, resolution)] = curr_prod_dict
        # Create a temporary directory
        temp_dir = output_dir / f"{product}_{epoch}_{crs}_{resolution}"
        temp_dir.mkdir(exist_ok=True)
        for tile in tile_id:
            curr_prod_dict["tiles"][tile] = {
                "url": build_tile_url(tile, product, epoch, crs, resolution),
                "zipfile": f"{product}_{epoch}_{crs}_{resolution}_{tile}.zip",
            }
        curr_prod_dict["dir"] = temp_dir
        curr_prod_dict["tiffile"] = f"{prefix}{product}_{epoch}_{crs}_{resolution}.tif"

    # Download and merge tiles
    file_list = []
    for product, values in product_dict.items():
        tiles_dict = values["tiles"]
        prod_dir = values["dir"]
        prod_tif = values["tiffile"]

        # Download tiles
        for tile_id, tile_values in tiles_dict.items():
            url = tile_values["url"]
            ofile = tile_values["zipfile"]
            print(f"Downloading {tile_id} for {product}... ", end="")
            urlretrieve(url, prod_dir / ofile)
            with ZipFile(prod_dir / ofile, "r") as zfile:
                ziplist = [f for f in zfile.filelist if f.filename.endswith(".tif")]
                assert len(ziplist) == 1
                tfile = ziplist[0]
                zfile.extract(tfile, prod_dir)
                tile_values["tiffile"] = tfile.filename
            # Remove zip
            (prod_dir / ofile).unlink()
            print("Done.")

        # Merge tiles if needed
        if extent == "bbox":
            bbox_file = Path(prod_dir / "bbox.csv")
            with open(bbox_file, "w", encoding="utf-8") as f:
                f.write("id,WKT\n")
                f.write(f'1,"{bbox.wkt}"\n')
            warp_options = gdal.WarpOptions(
                format="GTiff",
                creationOptions=["COMPRESS=LZW", "TILED=YES"],
                cutlineDSName=bbox_file,
                cropToCutline=True,
            )
        else:
            warp_options = gdal.WarpOptions(
                format="GTiff", creationOptions=["COMPRESS=LZW", "TILED=YES"]
            )
        if extent == "regions":
            # process and merge per region
            for region, rtiles in regions_dict.items():
                tile_list = [
                    prod_dir / t["tiffile"]
                    for tid, t in tiles_dict.items()
                    if tid in rtiles
                ]
                # vrt = gdal.BuildVRT(prod_dir / f"vrt_{region}.tif", tile_list)
                # gdal.Translate(
                #     output_dir / f"{region}_{prod_tif}",
                #  vrt, options=translate_options
                # )
                # vrt = None
                _ = gdal.Warp(
                    output_dir / f"{region}_{prod_tif}",
                    tile_list,
                    options=warp_options,
                )
                _ = None
                file_list.append(output_dir / f"{region}_{prod_tif}")
        else:
            # merge tiles using gdal
            # move merged file to parent dir
            tile_list = [prod_dir / t["tiffile"] for t in tiles_dict.values()]
            # vrt = gdal.BuildVRT(prod_dir / "vrt.tif", tile_list)
            # gdal.Translate(output_dir / prod_tif, vrt, options=translate_options)
            # vrt = None
            _ = gdal.Warp(
                output_dir / f"{prod_tif}",
                tile_list,
                options=warp_options,
            )
            _ = None
            file_list.append(output_dir / prod_tif)
        # remove temp dir
        shutil.rmtree(prod_dir)

    return file_list


def build_tile_url(tile_id, product, epoch, crs, resolution):
    """Generates url for global data file or tile in the GHSL FTP servers.

    Parameters
    ----------
    tile_id : str
        Id of the tile requested, if equal to "global" points to the global file.
    product : str
        Name of the GHSL product requested.
    epoch : int
        Year of the data requested.
    crs : int
        CRS code, must be 54009 or 4326.
    resolution : int
        Resolution in meteres, must be either 100 or 1000.

    Returns
    -------
    str
        url of the data product.
    """
    if product == "LAND":
        release = "R2022A"
    else:
        release = "R2023A"

    res_map = {100: "3ss", 1000: "30ss"}
    if crs == 4326:
        resolution = res_map[resolution]

    if product == "SMOD":
        version = "V2-0"
    else:
        version = "V1-0"
    base_url = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/"
    level1 = f"GHS_{product}_GLOBE_{release}/"
    level2 = f"GHS_{product}_E{epoch}_GLOBE_{release}_{crs}_{resolution}/"
    level3 = f"{version}/"
    if tile_id == "global":
        level4 = ""
        tile_id = ""
    else:
        level4 = "tiles/"
        tile_id = "_" + tile_id
    level5 = (
        f"GHS_{product}_E{epoch}_GLOBE_{release}_{crs}_{resolution}_"
        f"{version.replace('-', '_')}{tile_id}.zip"
    )

    return base_url + level1 + level2 + level3 + level4 + level5
