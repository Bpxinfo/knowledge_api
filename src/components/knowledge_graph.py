from collections import Counter
from typing import List, Dict
import json
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
from nltk import pos_tag

# Download required NLTK resources
# nltk.download('punkt')
# nltk.download('wordnet')
# nltk.download('averaged_perceptron_tagger')

# class GraphAnalyzer:
#     def __init__(self):
#         self.data = []
#         self.node_statistics = {}
#         self.connected_nodes = []
#         self.lemmatizer = WordNetLemmatizer()

#     def get_wordnet_pos(self, treebank_tag):
#         """Map NLTK POS tags to WordNet POS tags for better lemmatization."""
#         if treebank_tag.startswith('J'):
#             return wordnet.ADJ
#         elif treebank_tag.startswith('V'):
#             return wordnet.VERB
#         elif treebank_tag.startswith('N'):
#             return wordnet.NOUN
#         elif treebank_tag.startswith('R'):
#             return wordnet.ADV
#         else:
#             return wordnet.NOUN

#     def lemmatize_text(self, text: str) -> str:
#         """Lemmatize text using POS tagging for better accuracy."""
#         if not text or not isinstance(text, str):
#             return ""
        
#         # Tokenize and get POS tags
#         words = nltk.word_tokenize(text.lower().strip())
#         pos_tags = pos_tag(words)
        
#         # Lemmatize with POS tags
#         lemmatized_words = [
#             self.lemmatizer.lemmatize(word, self.get_wordnet_pos(pos))
#             for word, pos in pos_tags
#         ]
#         return ' '.join(lemmatized_words)

#     def calculate_node_size_and_color(self, occurrences: int, max_occurrences: int) -> Dict:
#         """Calculate node visualization properties based on occurrence frequency."""
#         # Node size between 10 and 50
#         min_size = 10
#         max_size = 50
#         size = min_size + (occurrences / max_occurrences) * (max_size - min_size)
        
#         # Color gradient from light to dark blue
#         color_intensity = int(255 * (occurrences / max_occurrences))
#         color = f"rgb({255 - color_intensity}, {255 - color_intensity}, 255)"
        
#         return {
#             'size': size,
#             'color': color
#         }

#     def preprocess_node(self, node: str) -> str:
#         """Preprocess a node by lemmatizing it."""
#         return self.lemmatize_text(node) if node and isinstance(node, str) else ''

#     def calculate_nodes(self, data: List[dict]) -> Dict:
#         """Process uploaded data and calculate node relationships with lemmatization."""
#         self.data = data
#         node_counter = Counter()
#         relationships = set()
#         self.connected_nodes = []
        
#         # First pass: Count lemmatized nodes
#         for triple in data:
#             source = self.preprocess_node(triple.get('source', ''))
#             target = self.preprocess_node(triple.get('target', ''))
#             relationship = self.preprocess_node(triple.get('relationship', ''))
            
#             # Skip if both nodes are empty
#             if not source and not target:
#                 continue
            
#             # Count valid nodes
#             if source:
#                 node_counter[source] += 1
#             if target:
#                 node_counter[target] += 1
#             if relationship:
#                 relationships.add(relationship)

#         # Calculate maximum occurrences for styling
#         max_occurrences = max(node_counter.values()) if node_counter else 1
        
#         # Second pass: Create connections with styles
#         for triple in data:
#             source = self.preprocess_node(triple.get('source', ''))
#             target = self.preprocess_node(triple.get('target', ''))
#             relationship = self.preprocess_node(triple.get('relationship', ''))
            
#             # Skip invalid connections
#             if not source or not target:
#                 continue
            
#             # Calculate styles for both nodes
#             source_style = self.calculate_node_size_and_color(node_counter[source], max_occurrences)
#             target_style = self.calculate_node_size_and_color(node_counter[target], max_occurrences)
            
#             # Add connection with styles
#             self.connected_nodes.append({
#                 'source': source,
#                 'target': target,
#                 'relationship': relationship,
#                 'sourceStyle': source_style,
#                 'targetStyle': target_style
#             })

#         # Calculate node roles and styles
#         node_roles = {}
#         for node in node_counter:
#             style = self.calculate_node_size_and_color(node_counter[node], max_occurrences)
#             node_roles[node] = {'style': style}
        
#         # Update statistics
#         self.node_statistics = {
#             'total_unique_nodes': len(node_counter),
#             'node_occurrences': dict(node_counter),
#             'unique_relationships': list(relationships),
#             'node_roles': node_roles,
#             'top_nodes': dict(node_counter.most_common(10))
#         }
        
#         return self.get_graph_data()

#     def analyze_components(self) -> Dict:
#         """Analyze graph components and identify clusters."""
#         adjacency = {}
#         for connection in self.connected_nodes:
#             source = connection['source']
#             target = connection['target']
            
#             if source not in adjacency:
#                 adjacency[source] = set()
#             if target not in adjacency:
#                 adjacency[target] = set()
                
#             adjacency[source].add(target)
#             adjacency[target].add(source)
        
#         node_degrees = {
#             node: len(connections)
#             for node, connections in adjacency.items()
#         }
        
#         return {
#             'node_degrees': node_degrees,
#             'central_nodes': dict(Counter(node_degrees).most_common(3)),
#             'total_connections': len(self.connected_nodes)
#         }

#     def get_graph_data(self) -> Dict:
#         """Get combined graph data for frontend visualization."""
#         component_analysis = self.analyze_components()
#         return {
#             'statistics': self.node_statistics,
#             'connections': self.connected_nodes,
#             'analysis': component_analysis
#         }
    
from collections import defaultdict, Counter
from typing import List, Dict, Set, Optional

class GraphAnalyzer:
    def __init__(self):
        self.nodes: Dict = {}  # Store nodes by node_id
        self.edges: Dict = {}  # Store edges by id
        self.feedback_groups: Dict = defaultdict(list)  # Group by feedback_id
        self.node_types: Dict[str, Set[str]] = defaultdict(set)  # Track node types
        self.relationship_types: Set[str] = set()  # Track unique relationships

    def load_data(self, data: Dict[str, List[Dict]]) -> None:
        """
        Load and organize graph data from the provided JSON structure.
        
        Args:
            data: Dictionary containing 'node' and 'edge' lists
        """
        # Process nodes
        for node in data.get('node', []):
            self.nodes[node['node_id']] = {
                'id': node['id'],
                'label': node['label'],
                'type': node['type'],
                'feedback_id': node['feedback_id']
            }
            self.node_types[node['type']].add(node['node_id'])
            self.feedback_groups[node['feedback_id']].append(('node', node['node_id']))

        # Process edges
        for edge in data.get('edge', []):
            self.edges[edge['id']] = {
                'source': edge['source'],
                'target': edge['target'],
                'relationship': edge['relationship'],
                'feedback_id': edge['feedback_id']
            }
            self.relationship_types.add(edge['relationship'])
            self.feedback_groups[edge['feedback_id']].append(('edge', edge['id']))

    def get_node_statistics(self) -> Dict:
        """Calculate various statistics about the nodes in the graph."""
        return {
            'total_nodes': len(self.nodes),
            'node_types_count': {
                type_name: len(nodes) 
                for type_name, nodes in self.node_types.items()
            },
            'feedback_groups_count': len(self.feedback_groups),
            'unique_node_types': list(self.node_types.keys())
        }

    def get_edge_statistics(self) -> Dict:
        """Calculate statistics about the edges in the graph."""
        relationship_counts = Counter(
            edge['relationship'] for edge in self.edges.values()
        )
        return {
            'total_edges': len(self.edges),
            'unique_relationships': list(self.relationship_types),
            'relationship_counts': dict(relationship_counts)
        }

    def get_node_connections(self, node_id: str) -> Dict:
        """Get all connections for a specific node."""
        incoming = []
        outgoing = []
        
        for edge_id, edge in self.edges.items():
            if edge['source'] == node_id:
                outgoing.append({
                    'edge_id': edge_id,
                    'target': edge['target'],
                    'relationship': edge['relationship']
                })
            if edge['target'] == node_id:
                incoming.append({
                    'edge_id': edge_id,
                    'source': edge['source'],
                    'relationship': edge['relationship']
                })
                
        return {
            'incoming': incoming,
            'outgoing': outgoing,
            'total_connections': len(incoming) + len(outgoing)
        }

    def get_feedback_group(self, feedback_id: int) -> Dict:
        """Get all nodes and edges for a specific feedback group."""
        group_items = self.feedback_groups[feedback_id]
        
        nodes = []
        edges = []
        
        for item_type, item_id in group_items:
            if item_type == 'node':
                nodes.append(self.nodes[item_id])
            else:  # edge
                edges.append(self.edges[item_id])
                
        return {
            'feedback_id': feedback_id,
            'nodes': nodes,
            'edges': edges
        }

    def find_paths(self, start_node: str, end_node: str, max_depth: int = 5) -> List[List[Dict]]:
        """Find all paths between two nodes up to a maximum depth."""
        def dfs(current: str, target: str, path: List, depth: int) -> List[List[Dict]]:
            if depth > max_depth:
                return []
            if current == target:
                return [path]
                
            paths = []
            for edge_id, edge in self.edges.items():
                if edge['source'] == current:
                    next_node = edge['target']
                    if next_node not in [p['target'] for p in path]:
                        new_path = path + [{
                            'edge_id': edge_id,
                            'source': current,
                            'target': next_node,
                            'relationship': edge['relationship']
                        }]
                        paths.extend(dfs(next_node, target, new_path, depth + 1))
            return paths
            
        return dfs(start_node, end_node, [], 0)

    def get_network_metrics(self) -> Dict:
        """Calculate basic network metrics."""
        # Calculate node degrees
        degrees = defaultdict(int)
        for edge in self.edges.values():
            degrees[edge['source']] += 1
            degrees[edge['target']] += 1
            
        # Find central nodes (highest degree)
        central_nodes = sorted(
            degrees.items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:5]
        
        return {
            'average_degree': sum(degrees.values()) / len(self.nodes),
            'max_degree': max(degrees.values()) if degrees else 0,
            'min_degree': min(degrees.values()) if degrees else 0,
            'central_nodes': [
                {
                    'node_id': node_id,
                    'degree': degree,
                    'node_info': self.nodes[node_id]
                }
                for node_id, degree in central_nodes
            ]
        }

    def analyze(self) -> Dict:
        """Perform complete analysis of the graph."""
        return {
            'node_statistics': self.get_node_statistics(),
            'edge_statistics': self.get_edge_statistics(),
            'network_metrics': self.get_network_metrics(),
            'feedback_groups_count': len(self.feedback_groups)
        }    

# analyzer = GraphAnalyzer()
# data = [
#     {
#         "feedback_id": 1,
#         "id": 1,
#         "relationship": "causes",
#         "source": "mistrust",
#         "target": "barrier"
#     },
#     # ... rest of your data
# ]
# # result = analyzer.calculate_nodes(data)
# print(json.dumps(result, indent=2))    