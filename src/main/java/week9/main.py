import json
import pandas as pd
from pyvi import ViTokenizer

if __name__ == '__main__':
    fRead = open("/home/quanghuy/Documents/data/data_3.json", "r", encoding="utf8")
    fWrite = open("/home/quanghuy/Documents/data/result/data_3.json", "a", encoding="utf8")
    Lines = fRead.readlines()
    count = 0
    f = lambda x: x.replace("_", " ")
    # Strips the newline character
    for line in Lines:
        count += 1
        #print("Line{}: {}".format(count, line.strip()))
        if count % 2 != 0:
            fWrite.write(line)
        else:
            json_object = json.loads(line.strip())
            #print(json_object["title"])
            xyz = ViTokenizer.tokenize(json_object["title"]).split()
            #print(list(map(f, xyz)))
            fWrite.write("{" + "\"suggest\": [\"" + '", "'.join(list(map(f, xyz))) + "\"]}\n")
    fRead.close()
    fWrite.close()