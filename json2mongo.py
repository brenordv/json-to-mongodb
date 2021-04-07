# -*- coding: utf-8 -*-
"""
Usage:
    json2mongo.py --connection-string=mongodb://... --database=admin --collection=myCollection --payload=FILE.JSON [--max-batch-size=100 --repeat=0]

Options:
    --connection-string=<conn string>   Connection string
    --database=<database>               Database name
    --collection=<collection name>      Collection to be used.
    --payload=<payload file>            Complete path to the JSON file that will be used.
    --max-batch-size=<max batch size>   Maximum number of documents that can be sent per batch. [default: 100]
    --repeat=<repeat times>             Number of times the script will repeat the send operation. [default: 0]


"""
import json
from datetime import datetime
from pathlib import Path
from typing import Union
from docopt import docopt
from pymongo import MongoClient
import random
from tqdm import tqdm
import copy

CLIENT = None
DATABASE = None
COLLECTION = None
ORIGINAL_CONTENT = None


def get_client(collection_name, connection_string, database_name):
    global CLIENT
    global DATABASE
    global COLLECTION

    if CLIENT is None:
        CLIENT = MongoClient(connection_string)

    if DATABASE is None:
        DATABASE = CLIENT[database_name]

    if COLLECTION is None:
        COLLECTION = DATABASE[collection_name]

    return COLLECTION


def read_json(payload_file: Path):
    global ORIGINAL_CONTENT

    if ORIGINAL_CONTENT is None:
        with open(payload_file, "r", encoding="utf-8") as file:
            ORIGINAL_CONTENT = json.load(file)

    return copy.deepcopy(ORIGINAL_CONTENT)


def parse_value_tokens(value, original: dict, parsed: dict):
    if not isinstance(value, str) \
            or len(value.strip()) == 0 \
            or "$" not in value:
        return value

    _value = value.lower().strip()
    if _value.startswith("$randbetween"):
        min_value, max_value = [int(v) for v in _value.replace("$randbetween(", "").replace(")", "").split(";")]
        return random.randint(min_value, max_value)

    if _value == "$now":
        return datetime.now()

    if _value == "$utcnow":
        return datetime.utcnow()

    if "$prop" in _value:
        token_start_index = _value.find("$prop")
        token_end_index = _value.find(")", token_start_index) + 1
        token = value[token_start_index+6: token_end_index-1]
        token_value = parsed.get(token, original.get(token))
        return value.replace(value[token_start_index: token_end_index], str(token_value))



    return value


def parse_content(json_content: Union[dict, list, tuple]) -> Union[dict, list, tuple]:
    if isinstance(json_content, (list, tuple)):
        parsed_list = list()
        [parsed_list.append(parse_content(item)) for item in json_content]
        return parsed_list

    parsed_item = dict()
    for key, value in json_content.items():
        parsed_item[key] = parse_value_tokens(value, json_content, parsed_item)

    return parsed_item


def get_payload(payload_file: Path) -> Union[dict, list, tuple]:
    json_content = read_json(payload_file)
    return parse_content(json_content)


def get_payload_chunks(lst, n):

    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def process_send(connection_string: str, database_name: str, collection_name: str, payload_file: Path,
                 limit_batch_size: int = 100):
    payload = get_payload(payload_file)
    collection = get_client(collection_name, connection_string, database_name)

    if isinstance(payload, (list, tuple)):
        chunks = list(get_payload_chunks(payload, limit_batch_size))
        with tqdm(total=len(chunks), desc="Sending data batch", position=1) as pbar:
            for chunk in chunks:
                pbar.update(1)
                collection.insert_many(chunk)
    else:

        collection.insert_one(payload)


def main(connection_string: str, database_name: str, collection_name: str, payload_file: Path, repeat: int = 0,
         limit_batch_size: int = 100):
    process_send(
        connection_string=connection_string,
        database_name=database_name,
        collection_name=collection_name,
        payload_file=payload_file,
        limit_batch_size=limit_batch_size)

    with tqdm(total=repeat, desc="Repeating operation", position=0) as pbar:
        for i in range(repeat):
            pbar.update(1)
            process_send(
                connection_string=connection_string,
                database_name=database_name,
                collection_name=collection_name,
                payload_file=payload_file,
                limit_batch_size=limit_batch_size)


if __name__ == '__main__':
    args = docopt(__doc__, help=True)
    main(
        connection_string=args["--connection-string"],
        database_name=args["--database"],
        collection_name=args["--collection"],
        payload_file=Path(args["--payload"]),
        repeat=int(args["--repeat"]),
        limit_batch_size=int(args["--max-batch-size"]))
