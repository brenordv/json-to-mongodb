# JSON 2 Mongodb
This scripts reads a json file and sends it to a MongoDb collection.
It has a couple of functions to manipulate data before sending.


## Using tokens in JSON
- Value of ```$now``` will be replaced by the current local time.
- Value of ```$utcnow``` will be replaced by the current time in UTC.

- Value of ```$randBetween(x;y)``` where x is the minimum and y is the maximum, the script 
will replace the entire string for a random int between those two numbers.

- If you add ```$prop(x)``` to any string value, it will be replaced by the value of x by it's value. 
  In this case, x is a key in the json. The script will try to get a parsed value for this key and if none are found,
  will use the original value. 
  Example:  
  ````json
    {
      "id": 42,
      "name": "FooBar-$prop(id)"
    }
  ````
  The json above will be translated to:
  ````json
    {
      "id": 42,
      "name": "FooBar-42"
    }
  ````


## Command line options
```shell
Usage:
    json2mongo.py --connection-string=mongodb://... --database=admin --collection=myCollection --payload=FILE.JSON [--max-batch-size=100 --repeat=0]

Options:
    --connection-string=<conn string>   Connection string
    --database=<database>               Database name
    --collection=<collection name>      Collection to be used.
    --payload=<payload file>            Complete path to the JSON file that will be used.
    --max-batch-size=<max batch size>   Maximum number of documents that can be sent per batch. [default: 100]
    --repeat=<repeat times>             Number of times the script will repeat the send operation. [default: 0]
```
Almost all options are self-explanatory, but the last two are _note-worthy_.
1. **max-batch-size**: If the payload file has a list of jsons, they will be split into smaller chunks of n items each. 
   In this case, n is the number you pass to this parameter. This was added to avoid problems when sending large lists to mongodb.
2. **repeat**: When this script is executed, it will do it's job and send data to mongodb. If repeat is greater than 0, it will send data again and again, according to the value passed here. (Example: if repeat is equal to 10, it will send data 11 times: The first one +10 repeats.)    


## Examples
### Sending the contents of .tmp/test_payload.json file 21 times (first time +20 repeats)
```shell
python json2mongo.py --connection-string="mongodb://..." --database=admin --collection=MonitoredStation --payload=.tmp/test_payload.json --repeat=20
```


### Sending the contents of .tmp/test_payload.json file only once and splitting the list in file into n lists of 50 items each.
```shell
python json2mongo.py --connection-string="mongodb://..." --database=admin --collection=MonitoredStation --payload=.tmp/test_payload.json --max-batch-size=50
```


## Installation
```shell
pip install -r requirements.txt
```

## 