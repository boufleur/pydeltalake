import os
import pydeltalake.lib

def test001_latest_manifest_exists():
    files_src = os.path.join(os.getcwd(), 'test', 'data', 'delta_format_adap')
    checkpoint_id = pydeltalake.lib.get_checkpoint_id(files_src)
    assert checkpoint_id != None

def test001_latest_manifest_not_exists():
    files_src = os.path.join(os.getcwd(), 'test', 'data', 'delta_test')
    checkpoint_id = pydeltalake.lib.get_checkpoint_id(files_src)
    assert checkpoint_id == None

def test002_removes():
    file_src = os.path.join(os.getcwd(), 'test', 'data', 'delta_test', '_delta_log', '00000000000000000001.json')
    with open(file_src) as file:
        adds, removes = pydeltalake.lib.replay_log(file)
    assert adds[0] == 'part-00000-f6a39956-2c3f-4c64-9a04-1393083bc46b-c000.snappy.parquet'