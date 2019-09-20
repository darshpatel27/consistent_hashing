from collections import defaultdict, Counter
from random import shuffle, randint, choice


class ConsistentHashing:

    VIRTUAL_NODES = 1000  # aka logical number of shards
    ACTUAL_NODES = 4  # aka physical number of shards
    HASH_RANGE = 10000  # increase/ decrease number of keys/size per logical shard

    CURRENT_ACTIVE_NODES = 4
    CURRENT_NODE_LIST = [i for i in range(1, ACTUAL_NODES + 1)]

    def __init__(self):
        self.node_shard_freq_list = []
        self.populate_node_shard_list()
        self.shard_size = int(self.HASH_RANGE / self.VIRTUAL_NODES)

        self.shards_to_nodes = {}
        self.map_shards_to_nodes()

    def populate_node_shard_list(self):
        # no of shards per node
        total_shards_per_node = int(self.VIRTUAL_NODES / self.ACTUAL_NODES)
        self.node_shard_freq_list = [node_id for node_id in range(1, self.ACTUAL_NODES + 1)] * total_shards_per_node

        # randomise node_shard_freq_list to randomly assign shards to physical nodes
        shuffle(self.node_shard_freq_list)

    def map_shards_to_nodes(self):
        for i in range(1, self.VIRTUAL_NODES + 1):
            self.shards_to_nodes[i*self.shard_size] = self.get_random_actual_node(i)

    def get_random_actual_node(self, index):
        return self.node_shard_freq_list[index-1]

    def remove_actual_node_from_shard_map(self, node_id):
        for key, val in self.shards_to_nodes.items():
            if val == node_id:
                self.shards_to_nodes[key] = None
        self.CURRENT_ACTIVE_NODES -= 1
        self.CURRENT_NODE_LIST.remove(node_id)

    def add_actual_node_to_shard_map(self, node_id):
        self.CURRENT_ACTIVE_NODES += 1
        self.CURRENT_NODE_LIST.append(node_id)
        actual_node_freq_count = int(self.VIRTUAL_NODES / self.CURRENT_ACTIVE_NODES)
        count = 0
        while actual_node_freq_count > 0:
            key_assign = None
            for i in range(1, self.CURRENT_ACTIVE_NODES+1):
                curr_key = count * (self.shard_size * self.CURRENT_ACTIVE_NODES) + self.shard_size * i
                if self.shards_to_nodes[curr_key] is None and not key_assign:
                    key_assign = curr_key
                    self.shards_to_nodes[key_assign] = node_id
                    # print(curr_key, self.shards_to_nodes[curr_key])
                    continue
                elif self.shards_to_nodes[curr_key] is None and key_assign:
                    self.shards_to_nodes[curr_key] = choice(self.CURRENT_NODE_LIST)
                    # print(curr_key, self.shards_to_nodes[curr_key])

            if not key_assign:
                key_assign = count * (self.shard_size * self.CURRENT_ACTIVE_NODES) + self.shard_size * randint(
                    1, self.CURRENT_ACTIVE_NODES)
                self.shards_to_nodes[key_assign] = node_id
            count += 1
            actual_node_freq_count -= 1

    def get_node_id_for_request(self, request_id):
        shard = request_id % self.HASH_RANGE
        node_id = None
        for key, val in self.shards_to_nodes.items():
            if key > shard and val:
                node_id = val
                break

        # if node_id not found, go back in circle till you find next available node
        if not node_id:
            for key, val in self.shards_to_nodes.items():
                if val:
                    node_id = val
                    break
        return node_id

