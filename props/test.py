import json

with open('config.json', 'r') as f:
    configuration = json.load(f)

    # a = configuration['SNS_email']

    # for i in a:
    #     print(i)
    #     print(type(i))
    print(type(str(configuration['tableList'])))