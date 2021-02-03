import os
import time
import json
import ndjson
import pandas
from fsspec.implementations.local import LocalFileSystem


class DeltaLake:
    def __init__(self, path: str, filesystem=None, time_travel=None):
        if not filesystem:
            self.filesystem = LocalFileSystem(path)
        else:
            self.filesystem = filesystem
        self.path = path
        self._set_timestamp(time_travel)
        self.checkpoint_info = self._get_checkpoint_info()
        self.fileset = set()

    def _set_timestamp(self, time_travel):
        if not time_travel:
            self.timestamp = None
        else:
            self.timestamp = round(time.mktime(time_travel.timetuple()))

    def _get_checkpoint_info(self):
        try:
            with self.filesystem.open(
                os.path.join(self.path, "_delta_log", "_last_checkpoint")
            ) as last_checkpoint:
                return json.load(last_checkpoint)
        except (FileNotFoundError, OSError):
            return None

    def _replay_log(self, file):
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

    def _delta_files(self, version=None):
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

    def _replay_delta_and_update_fileset(self, version=0):
        for file in self._delta_files(version):
            adds, removes = self._replay_log(file)
            self.fileset |= adds
            self.fileset -= removes

    def _get_checkpoint_files(self):
        if "parts" in self.checkpoint_info.keys():
            checkpoint_files = [
                f"{self.path}/_delta_log/{str(self.checkpoint_info['version']).zfill(20)}.checkpoint.{str(i).zfill(10)}.{str(self.checkpoint_info['parts']).zfill(10)}.parquet"
                for i in range(1, self.checkpoint_info["parts"] + 1)
            ]
        else:
            checkpoint_files = [
                f"{self.path}/_delta_log/{str(self.checkpoint_info['version']).zfill(20)}.checkpoint.parquet"
            ]
        return checkpoint_files

    def _get_checkpoints(self):
        # TODO: handle missing multi-part files
        checkpoints = []
        for checkpoint_file in self._get_checkpoint_files():
            with self.filesystem.open(checkpoint_file) as file_handler:
                checkpoints.append(pandas.read_parquet(file_handler))
        return checkpoints

    def _replay_checkpoint_and_update_fileset(self):
        for checkpoint in self._get_checkpoints():
            self.fileset |= set(
                x["path"] for x in checkpoint[checkpoint["add"].notnull()]["add"]
            )

    def files(self):
        replay_checkpoint = True
        if self.timestamp:  # time travel needs to replay all
            replay_checkpoint = False
            self._replay_delta_and_update_fileset()
        if self.checkpoint_info and replay_checkpoint:
            self._replay_checkpoint_and_update_fileset()
            self._replay_delta_and_update_fileset(self.checkpoint_info["version"] + 1)
        return self.fileset
