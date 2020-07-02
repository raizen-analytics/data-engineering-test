# -*- coding: utf-8 -*-

### Import Section

# Python built-in modules
import json,

# Project modules
import extractor, transform

# 3rd part modules
import logging

### Code Section
logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')

def importConfig(self):
    with open("config.json") as file:
        return json.load(file)

def main():
    configs = importConfig()
    ext = extractor.Extractor(configs['url'])
    data = ext.extract()
    # trs = transform.Transform(ext)
    # lod = load.Load(trs)
    # keyWord =
    pprint(data)

# if __file__ == "__main__":
main()
