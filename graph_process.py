import networkx as nx
G = nx.Graph()
G.add_edge('657d9fce-7a19-347d-ba1d-c3820918b4d3', '533f7f0e-2258-365e-b5ae-777a9df30c98', weight=1, selectedRelationships="parse.failure")
G.add_edge('e1ef5c7b-93e4-3b0a-9195-1f766bb9327c', '4b02820a-8bdb-3c84-bd13-883d045e9d4b', weight=1, selectedRelationships="success")
G.add_edge('fd611e66-6690-371f-8a1e-85c8bca4695d', 'e1ef5c7b-93e4-3b0a-9195-1f766bb9327c', weight=1, selectedRelationships="success")
G.add_edge('533f7f0e-2258-365e-b5ae-777a9df30c98', '533f7f0e-2258-365e-b5ae-777a9df30c98', weight=1, selectedRelationships="failure")
print(nx.shortest_path(G, 'A', 'D', weight='weight'))