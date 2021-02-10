"""A Python interface to Delta Lake

This library makes it possible to for users to connect to a storage endpoint
and read files using delta protocol. You can find more information about Delta
Lake on https://github.com/delta-io/delta/blob/master/PROTOCOL.md.

  Typical usage example:

  from pydeltalake.lib import DeltaLake

  dl = DeltaLake("/tests/data/delta-0.2.0")
  dl.files()
"""
import os
import time
import datetime
from typing import Dict, List, Set, TextIO, Tuple
import json
import ndjson
import pandas
from pandas import DataFrame
from fsspec.implementations.local import LocalFileSystem, AbstractFileSystem


class DeltaLake:
    """An instance of containing a Delta Lake

    This class provides an interface for Delta Lake
    using python-like filesystems provided by fsspec.

    """

    def __init__(
        self,
        path: str,
        filesystem: AbstractFileSystem = None,
        time_travel: datetime = None,
    ):
        """Initializes a Delta Lake

        Retrieves rows pertaining to the given keys from the Table instance
        represented by table_handle.  String keys will be UTF-8 encoded.

        Args:
            path: the path to the table on the filesystem
            filesystem: python-like filesystem (If unset, assume local)
            time_travel: set the delta lake to a specific version

        Returns:
            An instance of a delta table.
        """
        if not filesystem:
            self.filesystem = LocalFileSystem(path)
        else:
            self.filesystem = filesystem
        self.path = path
        self._set_timestamp(time_travel)
        self.checkpoint_info = self._get_checkpoint_info()
        self.fileset = set()

    def _set_timestamp(self, time_travel: datetime):
        if not time_travel:
            self.timestamp = None
        else:
            self.timestamp = round(time.mktime(time_travel.timetuple()))

    def _get_checkpoint_info(self) -> Dict:
        try:
            with self.filesystem.open(
                os.path.join(self.path, "_delta_log", "_last_checkpoint")
            ) as last_checkpoint:
                return json.load(last_checkpoint)
        except (FileNotFoundError, OSError):
            return None

    def _replay_log(self, file: TextIO) -> Tuple[Set, Set]:
        actions = ndjson.loads(file.read())

        if not self.timestamp:
            cut_time = round(time.time() * 1000)
        else:
            cut_time = self.timestamp * 1000

        adds = set(
            action["add"]["path"]
            for action in actions
            if "add" in action.keys() and action["add"]["modificationTime"] < cut_time
        )
        removes = set(
            action["remove"]["path"]
            for action in actions
            if "remove" in action.keys()
            and action["remove"]["deletionTimestamp"] < cut_time
        )

        return adds, removes

    def _delta_files(self, version: int = None) -> str:
        if not version:
            version = 0
        while True:
            try:
                loc = f"{self.path}/_delta_log/{str(version).zfill(20)}.json"
                file = self.filesystem.open(loc)
                version += 1
                yield file
            except (FileNotFoundError, OSError):
                break

    def _replay_delta_and_update_fileset(self, version: int = 0):
        for file in self._delta_files(version):
            adds, removes = self._replay_log(file)
            self.fileset |= adds
            self.fileset -= removes

    def _get_checkpoint_files(self) -> List[str]:
        if "parts" in self.checkpoint_info.keys():
            checkpoint_files = [
                f"{self.path}/_delta_log/"
                + f"{str(self.checkpoint_info['version']).zfill(20)}"
                + f".checkpoint.{str(i).zfill(10)}"
                + f".{str(self.checkpoint_info['parts']).zfill(10)}.parquet"
                for i in range(1, self.checkpoint_info["parts"] + 1)
            ]
        else:
            checkpoint_files = [
                f"{self.path}/_delta_log/"
                + f"{str(self.checkpoint_info['version']).zfill(20)}.checkpoint.parquet"
            ]
        return checkpoint_files

    def _get_checkpoint(self) -> DataFrame:
        checkpoints = []
        for checkpoint_file in self._get_checkpoint_files():
            with self.filesystem.open(checkpoint_file) as file_handler:
                checkpoints.append(pandas.read_parquet(file_handler))
        return pandas.concat(checkpoints)

    def _replay_checkpoint_and_update_fileset(self):
        checkpoint = self._get_checkpoint()
        self.fileset |= set(
            x["path"] for x in checkpoint[checkpoint["add"].notnull()]["add"]
        )

    def files(self) -> Set:
        """Fetches the parquet file list from the delta lake.

        Provides a list of the parquet files on the delta lake on the
        date specified during instantiation.

        Returns:
            A set of the parquet files on the delta lake.

        """
        if (
            self.timestamp or not self.checkpoint_info
        ):  # time travel needs to replay all
            self._replay_delta_and_update_fileset()
        else:
            self._replay_checkpoint_and_update_fileset()
            self._replay_delta_and_update_fileset(self.checkpoint_info["version"] + 1)
        return self.fileset
