import time
import requests as r
import json
import pandas as pd
import numpy as np
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


def _overtaking(df: pd.DataFrame):
    pivot = df.pivot("lap", "driver_name", "cum_lap_time")
    # Create deep copy TODO: create empty dataframe
    overtaken = pivot.copy(deep=True)
    overtaking = pivot.copy(deep=True)
    clean_lap = pivot.copy(deep=True)
    curr_lap = pivot.fillna(np.Infinity)
    next_lap = pivot.shift(1).fillna(np.Infinity)
    cols = list(overtaken.columns.copy(deep=True))
    for driver in cols:
        overtaken[driver] = np.empty((len(pivot), 0)).tolist()
        overtaking[driver] = np.empty((len(pivot), 0)).tolist()
        clean_lap[driver] = True
        for opponent in cols:
            if driver != opponent:
                # Compute if one driver got overtaken during the lap
                overtaken_opponent_driver_cond = (
                    curr_lap[driver] < curr_lap[opponent]
                ) & (next_lap[driver] > next_lap[opponent])
                # Compute if driver overtake during the lap
                overtaking_opponent_driver_cond = (
                    curr_lap[opponent] < curr_lap[driver]
                ) & (next_lap[opponent] > next_lap[driver])
                # Clean lap is approximation, if driver is less than 3 seconds behind opponent, then lap is not clean
                clean_lap_opponent_driver_cond = (
                    curr_lap[driver] - curr_lap[opponent] > 3
                ) | (curr_lap[driver] - curr_lap[opponent] < 0)
                # Add overtaking and overtaken
                overtaken.loc[overtaken_opponent_driver_cond, driver] = overtaken.loc[overtaken_opponent_driver_cond, driver]\
                    .apply(lambda x: x + [opponent])
                overtaking.loc[overtaking_opponent_driver_cond, driver] = overtaking.loc[overtaking_opponent_driver_cond, driver]\
                    .apply(lambda x: x + [opponent])
                # Boolean for clean driver
                clean_lap[driver] = clean_lap[driver] & clean_lap_opponent_driver_cond
    # Join all dataframe
    df_indexed = df.set_index(["driver_name", "lap"])
    df_overtaken = df_indexed.join(
        overtaken.melt(ignore_index=False)
        .rename({"value": "overtaking"}, axis=1)
        .reset_index()
        .set_index(["driver_name", "lap"]),
        how="left",
    )
    df_overtaking = df_overtaken.join(
        overtaking.melt(ignore_index=False)
        .rename({"value": "overtaken_by"}, axis=1)
        .reset_index()
        .set_index(["driver_name", "lap"]),
        how="left",
    )
    df_cleanlap = df_overtaking.join(
        clean_lap.melt(ignore_index=False)
        .rename({"value": "clean_lap"}, axis=1)
        .reset_index()
        .set_index(["driver_name", "lap"]),
        how="left",
    )
    return df_cleanlap.reset_index()


def lap(series="f1", year=2021, max_round=99):
    """
    Create CSV files for an entire season.

    Return number of round processed
    """
    checkandcreatefolder("lap", series, year)
    t = time.time()
    for round in range(1, max_round + 1):
        if t + 1 / 4 - time.time() > 0:
            time.sleep(t + 1 / 4 - time.time())
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
        # Compute overtaking and clean lap
        df = _overtaking(df)
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
        if t + 1 / 4 - time.time() > 0:
            time.sleep(t + 1 / 4 - time.time())
        t = time.time()
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
            pitstops.to_csv(f"data/pitstop/{series}/{year}/{round}.csv", index=None)
        else:
            raise Exception("Network Error")
    return round


def lap_pistop_merge(series="f1", year=2021, max_round=99):
    laps = pd.read_csv(f"data/lap/{series}/{year}/{round}.csv")
    pitstops = pd.read_csv(f"data/pitstop/{series}/{year}/{round}.csv")
    lapsSorted = laps.set_index(["driver_name", "lap"]).sort_index()
    pitstopsSorted = pitstops.set_index(["driver_name", "lap"]).sort_index()
    # Merge both dataset and fill all blank with correct value
    laps_pitstop = lapsSorted.join(pitstopsSorted)
    laps_pitstop["pit_time"] = laps_pitstop["pit_time"].fillna(0)
    laps_pitstop["pit_amount"] = (
        (laps_pitstop["pit_amount"].groupby("driver_name").ffill().fillna(0))
        .groupby("driver_name")
        .shift(1, fill_value=0)
    )
    laps_pitstop["stop"] = laps_pitstop["stop"].fillna(False)
    # Get tyre lap old
    laps_pitstop["tyre_lap_old_race"] = (
        laps_pitstop.groupby(["driver_name", "pit_amount"]).cumcount() + 1
    )
    # Save to CSV
    laps_pitstop = laps_pitstop.reset_index()
    laps_pitstop.to_csv(f"data/lap_pitstops/{series}/{year}/{round}.csv", index=None)


if __name__ == "__main__":
    for year in range(2014, 2022):
        print(f"Working for year {year}")
        lap(year=year)
        print("Lap Done")
        pitstop(year=year)
        print("Pit stop Done")
        lap_pistop_merge(year=year)
        print("Merge done")
