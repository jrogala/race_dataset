import time
import requests as r
import json
import pandas as pd
import os


def checkandcreatefolder(typeofdata: str, series: str, year: int):
    """
    Check and create folder if missing for the dataset
    """
    if not os.path.isdir("data"):
        os.mkdir("data")
    if not os.path.isdir(f"data/{typeofdata}"):
        os.mkdir(f"data/{typeofdata}")
    if not os.path.isdir(f"data/{typeofdata}/{series}"):
        os.mkdir(f"data/{typeofdata}/{series}")
    if not os.path.isdir(f"data/{typeofdata}/{series}/{year}"):
        os.mkdir(f"data/{typeofdata}/{series}/{year}")


def minutesecondsToSeconds(s: str):
    """
    Convert a xx:xx:xx or xx:xx.xx or xx.xx string to float
    """
    splited = s.split(":")
    if len(splited) == 1:
        return float(s)
    elif len(splited) == 2:
        return int(splited[0]) * 60 + float(splited[1])
    elif len(splited) == 3:
        return int(splited[0]) * 360 + float(splited[1]) * 60 + float(splited[2])


def lap(series="f1", year=2021, max_round=99):
    """
    Create CSV files for an entire season.

    Return number of round processed
    """
    checkandcreatefolder("lap", series, year)
    t = time.time()
    for round in range(1, max_round + 1):
        if t + 1/4 - time.time() > 0:
            time.sleep(t + 1/4 - time.time())
        t = time.time()
        rawlaps = r.get(
            f"http://ergast.com/api/{series}/{year}/{round}/laps.json?limit=99999"
        )
        if rawlaps.status_code == 200:
            jsonlaps = json.loads(rawlaps.content)
        else:
            raise Exception("Network Error")
        if int(jsonlaps["MRData"]["total"]) == 0:
            break
        dfs = []
        for race in jsonlaps["MRData"]["RaceTable"]["Races"]:
            racename = race["raceName"]
            for lap in race["Laps"]:
                num = lap["number"]
                timings = lap["Timings"]
                dfi = pd.DataFrame(timings)
                dfi["race_name"] = racename
                dfi["lap"] = num
                dfi["lap_time"] = dfi["time"].apply(minutesecondsToSeconds)
                dfi["driver_name"] = dfi["driverId"]
                dfi = dfi.loc[
                    :, ["race_name", "driver_name", "lap", "lap_time", "position"]
                ]
                dfs.append(dfi)
        df = pd.concat(dfs, ignore_index=True)
        df["cum_lap_time"] = (
            df.loc[:, ["driver_name", "lap_time"]].groupby("driver_name").cumsum()
        )
        df.to_csv(f"data/lap/{series}/{year}/{round}.csv", index=None)
    return round


def pitstop(series="f1", year=2021, max_round=99):
    """
    Create CSV files for an entire season. Adding pitstops

    Return number of round processed
    """
    checkandcreatefolder("pitstop", series, year)
    t = time.time()
    for round in range(1, max_round + 1):
        if t + 1/4 - time.time() > 0:
            time.sleep(t + 1/4 - time.time())
        t = time.time()
        try:
            laps = pd.read_csv(f"data/lap/{series}/{year}/{round}.csv")
        except FileNotFoundError:
            # We have no more lap file so we are at the end
            break
        rawpitstops = r.get(
            f"http://ergast.com/api/{series}/{year}/{round}/pitstops.json?limit=99999"
        )
        if rawpitstops.status_code == 200:
            jsonpitstops = json.loads(rawpitstops.content)
            if int(jsonpitstops["MRData"]["total"]) == 0:
                pitstops = pd.DataFrame(
                    [], columns=["duration", "driverId", "stop", "lap"]
                )
            else:
                pitstops = pd.DataFrame(
                    jsonpitstops["MRData"]["RaceTable"]["Races"][0]["PitStops"]
                )
            # Pitstop work
            # pitstops["time_pit"] = pitstops["time"].apply(minutesecondsToSeconds)
            pitstops["pit_time"] = pitstops["duration"].apply(minutesecondsToSeconds)
            pitstops["lap"] = pd.to_numeric(pitstops["lap"])
            pitstops = pitstops.rename(
                {"driverId": "driver_name", "stop": "pit_amount"}, axis=1
            )
            pitstops["stop"] = pitstops["pit_time"] > 0
            pitstops = pitstops.loc[
                :, ["driver_name", "lap", "pit_amount", "stop", "pit_time"]
            ]
            # Multi index (driver_name, lap)
            lapsSorted = laps.set_index(["driver_name", "lap"]).sort_index()
            pitstopsSorted = pitstops.set_index(["driver_name", "lap"]).sort_index()
            # Merge both dataset and fill all blank with correct value
            laps_pitstop = lapsSorted.join(pitstopsSorted)
            laps_pitstop["pit_time"] = laps_pitstop["pit_time"].fillna(0)
            laps_pitstop["pit_amount"] = (
                laps_pitstop["pit_amount"].groupby("driver_name").ffill().fillna(0)
            ).groupby("driver_name").shift(1, fill_value=0)
            laps_pitstop["stop"] = laps_pitstop["stop"].fillna(False)
            # Get tyre lap old
            laps_pitstop["tyre_lap_old_race"] = laps_pitstop.groupby(["driver_name", "pit_amount"]).cumcount() + 1
            # Save to CSV
            laps_pitstop = laps_pitstop.reset_index()
            laps_pitstop.to_csv(f"data/pitstop/{series}/{year}/{round}.csv", index=None)
        else:
            raise Exception("Network Error")
    return round


if __name__ == "__main__":
    for year in range(2014, 2022):
        lap(year=year)
        pitstop(year=year)
