from consistent_hashing import ConsistentHashing
from collections import defaultdict, Counter


my_consistent_hash = ConsistentHashing()
request_count_map = defaultdict(int)
old_request_id_map = {}
for request_id in range(1, 10001):
    node_id = my_consistent_hash.get_node_id_for_request(request_id)
    request_count_map[node_id] += 1
    old_request_id_map[request_id] = node_id


print('\nTIME=t0\n---------------------------------')
print('SHARDS_COUNT PER NODE')
print(dict(Counter(my_consistent_hash.shards_to_nodes.values())))

print('\nREQUEST_COUNT PER NODE ')
print(dict(request_count_map))
print('---------------------------------\n')


def remove_nodes_from_cluster(node_list):
    for curr_node_id in node_list:
        # REMOVING ONE NODE FROM THE CLUSTER
        my_consistent_hash.remove_actual_node_from_shard_map(curr_node_id)

        request_count_map = defaultdict(int)
        new_request_id_map = {}
        hit = 0
        miss = 0
        for request_id in range(1, 10001):
            node_id = my_consistent_hash.get_node_id_for_request(request_id)
            request_count_map[node_id] += 1
            new_request_id_map[request_id] = node_id
            if old_request_id_map[request_id] == node_id:
                hit += 1
            else:
                miss += 1
        print('\n\nRemoving node={} \n--------------------------'.format(curr_node_id))
        print('SHARDS_COUNT PER NODE')
        new_shard_dist = defaultdict(int)
        count = 1
        for key, val in my_consistent_hash.shards_to_nodes.items():
            if val:
                new_shard_dist[val] += count
                count = 1
            else:
                count += 1

        print(dict(new_shard_dist))

        print('\nREQUEST_COUNT PER NODE ')
        print(dict(request_count_map))

        print('\nCACHE HIT : {}, CACHE MISS : {}'.format(hit, miss))
        print('\n')


def add_nodes_to_cluster(node_list):
    for curr_node_id in node_list:
        my_consistent_hash.add_actual_node_to_shard_map(curr_node_id)

        request_count_map = defaultdict(int)
        new_request_id_map = {}
        hit = 0
        miss = 0
        for request_id in range(1, 10001):
            node_id = my_consistent_hash.get_node_id_for_request(request_id)
            request_count_map[node_id] += 1
            new_request_id_map[request_id] = node_id
            if old_request_id_map[request_id] == node_id:
                hit += 1
            else:
                miss += 1
        print('\n\nAdding node={} \n--------------------------'.format(curr_node_id))

        print('SHARDS_COUNT PER NODE')
        print(dict(Counter(my_consistent_hash.shards_to_nodes.values())))

        print('\nREQUEST_COUNT PER NODE ')
        print(dict(request_count_map))

        print('\nCACHE HIT : {}, CACHE MISS : {}'.format(hit, miss))
        print('\n')


remove_nodes_from_cluster([3, 4])
#add_nodes_to_cluster([5, 6])


