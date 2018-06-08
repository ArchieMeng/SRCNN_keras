import pickle
import os
from time import time
from zipfile import ZipFile, ZIP_DEFLATED
from io import IOBase
from collections import defaultdict
from datasketch import MinHash, MinHashLSHEnsemble

file = 'words.pickle'
module_path = os.path.dirname(__file__)


def iter_data(file):
    """
    iter pickle data in file
    :param file: file name or IOBase object
    :return: generator of object in file
    """
    if isinstance(file, str):
        rf = open(file, 'rb')
    elif isinstance(file, IOBase):
        rf = file
    else:
        raise ValueError("file must be str or IOBase")
    while 1:
        try:
            data = pickle.load(rf)
            yield data
        except EOFError:
            break
    rf.close()
    

def iter_zip(zip_name):
    with ZipFile(zip_name, 'r') as zip_name:
        for name in zip_name.namelist():
            with zip_name.open(name, 'r') as rf:
                yield rf


def iter_category():
    id_set = set()
    for file in os.listdir('.'):
        if file.endswith('.pickle'):
            i = 0
            for shop in iter_data(file):
                id_set.add(shop['id'])
                i += 1
            print(file[:-7].replace('_', '\t'), "\t", i)
    print(len(id_set))


def gen_locations(locations_file='geo_location.txt'):
    """
    a generator which generate location pair by given file name
    :param locations_file: relative path to location file (relative to data_operation's path)
    :return:
    """
    with open(module_path + '/' + locations_file, 'r') as rf:
        for line in rf:
            if line:
                yield tuple(line.replace('\n', '').split(','))


def dump_data_loop(queue, stop_signal, zip_file_name):
    """
    used for zipping data to disk in a separate process

    :param queue: A multiprocessing Queue which initialized in main process and used for zip process.
    Data in queue must be (name, poi_id)

    :param stop_signal: A multiprocessing.Event class indicates whether main process is stopped

    :param zip_file_name: The name of zip file to create. (add '.zip' automatically)

    :return:
    """
    print("zip loop start with ", zip_file_name)
    while not (stop_signal.set() and queue.empty()):
        poi_id, data = queue.get()
        data['save_time'] = time()
        data['id'] = poi_id
        with ZipFile('{}.zip'.format(zip_file_name), 'a', ZIP_DEFLATED) as zip_file:
            file_name = '{}.pickle'.format(poi_id)
            while file_name in zip_file.namelist():
                file_name += '.1'
            with zip_file.open(file_name, 'w') as wf:
                pickle.dump(data, wf, protocol=pickle.HIGHEST_PROTOCOL)
                
                
def get_comment_dict(file_name):
    with ZipFile('meituan_comments.zip', 'r') as zip_file:
        return pickle.loads(zip_file.read(file_name))
  

def get_comment_type_count_score(file_name):
    comment = get_comment_dict(file_name)
    comment_types = {score_type['comment_score_type']: score_type['total_count'] for score_type in comment['comment_score_type_infos']}
    total = comment_types.get(0, 0)
    comment_types = [comment_types.get(i, 0) for i in range(1, 6)]
    score = comment['comment_score'] * total
    return comment_types, score


def shingling_str(sentence, width=2):
    return [sentence[i:i + 2] for i in range(len(sentence) - width + 1)]


def init_minhash(iterable, num_perm=128):
    min_hash = MinHash(num_perm=num_perm)
    for val in shingling_str(iterable):
        min_hash.update(val.encode())
    return min_hash


if __name__ == '__main__':
    print(len([*gen_locations('loc_hangzhou.txt')]))
