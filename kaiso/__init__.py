""" A graph based query and persistance framework
for objects and their relationships.

TODO: describe the architecture
"""


try:
    VERSION = __import__('pkg_resources').get_distribution('kaiso').version
except:
    VERSION = 'unknown'
