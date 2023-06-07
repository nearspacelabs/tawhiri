"""
Downloads forecast data and denerates a dataset (5-dimensional 32-bit value array) consisting of wind
(U and V components) and geopotential height forecast values suitable for consumption by Tawhiri.

Dataset axes: [forecast hour, pressure level, variable (U, V, GH), latitude, longitude]

"""
import asyncio
import os
import shutil
import sys
import tempfile
from datetime import datetime, timedelta
from time import sleep
from typing import Iterable, Optional, Tuple
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import aiohttp
import numpy as np
import yaml
import xarray as xr


class WindDataset:

    local_file_name: str = '{fcst_hr}.grib2'

    async def download_forecast_data(self):
        raise NotImplementedError

    def create_dataset(self, output_file: str):
        raise NotImplementedError

    def load_config(self, config_path: str):
        with open(config_path) as f:
            cfg = yaml.load(f, Loader=yaml.SafeLoader)
        self.pressures = sorted(cfg["pressure_levels"], reverse=True)
        self.variables = cfg["variables"]
        self.hours = list(range(cfg["min_fcst_hr"], cfg["max_fcst_hr"] + cfg["fcst_step"], cfg["fcst_step"]))

    async def _download_data(
        self, session: aiohttp.ClientSession, url: str, byte_range: Optional[Tuple[int, int]] = None
    ):
        headers = {"Range": f"bytes={byte_range[0]}-{byte_range[1]}"} if byte_range is not None else {}
        async with session.get(url, headers=headers) as response:
            return await response.read()

    async def _download_fields(
        self, grib_url: str, fcst_hr: int, data_vars: Iterable[str], session: aiohttp.ClientSession
    ):
        idx_url = grib_url + ".idx"
        idx_data = await self._download_data(session, idx_url)
        idx_rows = idx_data.decode().split("\n")

        byte_ranges = {}
        for i in range(len(idx_rows)):
            row = idx_rows[i]
            if row.strip():
                msg_id, first_byte, date, var, level, fcst, _ = row.split(":")
                try:
                    next_first_byte = idx_rows[i + 1].split(":")[1]
                except IndexError:
                    next_first_byte = None
                if var in data_vars:
                    byte_range = (int(first_byte), int(next_first_byte) - 1)
                    try:
                        byte_ranges[level][var] = byte_range
                    except KeyError:
                        byte_ranges[level] = {var: byte_range}

        grib_file = self.local_file_name.format(fcst_hr=fcst_hr)
        with open(grib_file, "ab") as w:
            for level, _vars in byte_ranges.items():
                if level.endswith("mb") and len(_vars) == len(
                    data_vars
                ):  # Only download levels where all data_vars exist
                    for var, byte_range in _vars.items():
                        w.write(await self._download_data(session, grib_url, byte_range))

    def __init__(self, model_run: datetime, config_path: str):
        self.model_run = model_run
        self.load_config(config_path)



class GFS(WindDataset):

    grib_url = "https://noaa-gfs-bdp-pds.s3.amazonaws.com/"\
               "gfs.{ymd}/{h:02d}/atmos/gfs.t{h:02d}z.{sub}.0p50.f{fcst_hr:03d}"

    async def download_forecast_data(self, max_concurrent_downloads: int = 10):
        grib_subs = ["pgrb2", "pgrb2b"]
        fields = ["UGRD", "VGRD", "HGT"]

        async with aiohttp.ClientSession() as session:
            coroutines = []
            for sub in grib_subs:
                for fcst_hr in self.hours:
                    _grib_url = self.grib_url.format(
                        ymd=self.model_run.strftime("%Y%m%d"),
                        h=self.model_run.hour,
                        fcst_hr=fcst_hr,
                        sub=sub
                    )
                    coroutines.append(self._download_fields(_grib_url, fcst_hr, fields, session))
            x = 0
            while x < len(coroutines):
                await asyncio.gather(*coroutines[x : x + max_concurrent_downloads])
                x += max_concurrent_downloads

    def create_dataset(self, output_file):
        """
        Create the 5D array dataset file.
        The file is created in a tmp location then moved to the final location for atomicity
        All source GRIB data is deleted during this operation
        """
        with tempfile.NamedTemporaryFile() as f:
            for hour in self.hours:
                grib_file = self.local_file_name.format(fcst_hr=hour)
                ds = xr.open_dataset(
                    grib_file, engine="cfgrib", backend_kwargs={"filter_by_keys": {"typeOfLevel": "isobaricInhPa"}}
                )
                # Tawhiri expects latitude and longitude to be increasing
                ds = ds.sortby("longitude").sortby("latitude")
                for p in self.pressures:
                    for v in self.variables:
                        ds[v].sel(isobaricInhPa=p).values.tofile(f)

                # Clean up original grib files and any intermediary index files
                os.remove(grib_file)
                try:
                    os.remove(grib_file + ".923a8.idx")
                except FileNotFoundError:
                    pass
            shutil.copy(f.name, output_file)

    @classmethod
    def get_latest_available_model_run(cls, min_required_fcst_hr: int, max_age_hrs: int = 24) -> datetime:
        """
        Find the most recently available model run
        min_required_fcst_hr: The minimum forecast hour that must be present on the remote server.
        max_age_hrs: Maximum allowed age of a model run.
        """
        now = datetime.utcnow()
        for model_run in previous_model_runs(now, 6):
            _grib_url = cls.grib_url.format(
                ymd=model_run.strftime("%Y%m%d"),
                h=model_run.hour,
                fcst_hr=min_required_fcst_hr,
                sub='pgrb2'
            )
            print(_grib_url)
            req = Request(_grib_url, method="HEAD")
            try:
                resp = urlopen(req)
                return model_run
            except HTTPError as e:
                if (now - model_run).total_seconds() / 3600 > max_age_hrs:
                    raise RuntimeError("No recent forecast found")


def previous_model_runs(dt: datetime, model_run_cadence: int, max_days: int = 30) -> datetime:
    """
    Generator that returns model run datetimes prior to dt.
    model_run_cadence: the number of hours between model runs (i.e. 6 for GFS, 1 for HRRR)
    max_days: max number of days to go back.
    """
    closest_previous_hour = dt.hour - dt.hour % model_run_cadence
    model_run = dt.replace(hour=closest_previous_hour, minute=0, second=0, microsecond=0)
    while dt - model_run < timedelta(days=max_days):
        yield model_run
        model_run -= timedelta(hours=model_run_cadence)


def main():
    output_dir = "/srv/tawhiri-datasets"
    config_file = sys.argv[1] #"tawhiri/configs/gfs0p5_config.yaml"
    model_run = GFS.get_latest_available_model_run(192)
    output_file = os.path.join(output_dir, model_run.strftime("%Y%m%d%H"))
    if os.path.exists(output_file):
        print('Latest forecast already processed - exiting')
        sys.exit(0)
    ds = GFS(model_run, config_file)
    asyncio.run(ds.download_forecast_data())
    ds.create_dataset(output_file)

    # Sleep 10 seconds to allow any lingering requests to be processed then delete any old datasets
    sleep(10)
    for f in os.listdir(output_dir):
        if os.path.join(output_dir, f) != output_file:
            os.remove(os.path.join(output_dir, f))

if __name__ == "__main__":
    main()
