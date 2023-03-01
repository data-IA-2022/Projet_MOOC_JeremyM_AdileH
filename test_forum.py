import json
import utils

print('---------------------------------------------------')
with open('sample.json') as f:
    for line in f:
        x = json.loads(line)
        utils.nbr_msg(x)
        # print(json.dumps(x, indent=4))

def test_forum():
    assert False
    pass